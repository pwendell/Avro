/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.avro.ipc.trace;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.ipc.HttpServer;
import org.apache.avro.ipc.RPCContext;
import org.apache.avro.ipc.RPCPlugin;
import org.apache.avro.specific.SpecificResponder;
import org.apache.avro.util.Utf8;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.ServletHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A tracing plugin for Avro.
 * 
 * This plugin traces RPC call timing and follows nested trees of RPC
 * calls. To use, instantiate and add to an existing Requestor or
 * Responder. If you have a Responder that itself acts as an RPC client (i.e.
 * it contains a Requestor) be sure to pass the same instance of the 
 * plugin to both components so that propagation occurs correctly.
 * 
 * Currently, propagation only works if each requester is performing
 * serial RPC calls. That is, we cannot support the case in which the
 * Requestor's request does not occur in the same thread as the Responder's
 * response. 
 */
public class TracePlugin extends RPCPlugin {
  final private static Random RANDOM = new Random();
  private static Utf8 HOSTNAME;
  private static final Logger LOG = LoggerFactory.getLogger(TracePlugin.class);
  public static enum StorageType { MEMORY, DISK };
  
  /*
   * This plugin uses three key/value meta-data pairs. The value type for all
   * of these pairs is fixed(8) and they are assumed to be encoded as 8-byte
   * ID's. The presence of a TRACE_ID_KEY and a SPAN_ID_KEY in a message
   * signals that tracing is in progress. The optional PARENT_SPAN_ID_KEY
   * signals that this message has a parent node in the RPC call tree. 
   */
  private static final Utf8 TRACE_ID_KEY = new Utf8("traceID");
  private static final Utf8 SPAN_ID_KEY = new Utf8("spanID");
  private static final Utf8 PARENT_SPAN_ID_KEY = new Utf8("parentSpanID");
  
  // Get the size of an RPC payload
  private static int getPayloadSize(List<ByteBuffer> payload) {
    if (payload == null) {
      return 0;
    }
    int size = 0;
    for (ByteBuffer bb: payload) {
      size = size + bb.limit();
    }
    return size;
  }
  
  /**
   * Create a span without any events. If traceID or spanID is null, randomly
   * generate them. If parentSpanID is null, assume this is a root span.
   */
  private Span createEventlessSpan(ID traceID, ID spanID, ID parentSpanID) {
    Span span = new Span();
    span.complete = false;
    
    if (traceID == null) {
      byte[] traceIDBytes = new byte[8];
      RANDOM.nextBytes(traceIDBytes);
      span.traceID = new ID();
      span.traceID.bytes(traceIDBytes);
    } else {
      span.traceID = new ID();
      span.traceID.bytes(traceID.bytes().clone());
    }
    
    if (spanID == null) {
      byte[] spanIDBytes = new byte[8];
      RANDOM.nextBytes(spanIDBytes);
      span.spanID = new ID();
      span.spanID.bytes(spanIDBytes);
    } else {
      span.spanID = new ID();
      span.spanID.bytes(spanID.bytes().clone());
    }

    if (parentSpanID != null) {
      span.parentSpanID = new ID();
      span.parentSpanID.bytes(parentSpanID.bytes().clone());
    }
    
    span.events = new GenericData.Array<TimestampedEvent>(
        10, Schema.createArray(TimestampedEvent.SCHEMA$));
    span.requestorHostname = HOSTNAME;
    
    return span;
  }
  
  // Add a timestampted event to this span
  private static void addEvent(Span span, SpanEvent eventType) {
    TimestampedEvent ev = new TimestampedEvent();
    ev.event = eventType;
    ev.timeStamp = System.currentTimeMillis() * 1000000;
    span.events.add(ev);
  }
  
  /**
   * Get the long value from a given ID object.
   */
  public static long longValue(ID in) {
    if (in == null) { 
      throw new IllegalArgumentException("ID cannot be null");
    }
    if (in.bytes() == null) {
      throw new IllegalArgumentException("ID cannot be empty");
    }
    if (in.bytes().length != 8) {
      throw new IllegalArgumentException("ID must be 8 bytes");
    }
    ByteBuffer buff = ByteBuffer.wrap(in.bytes());
    return buff.getLong();
  }
  
  /**
   * Get an ID associated with a given long value. 
   */
  public static ID IDValue(long in) {
    byte[] bArray = new byte[8];
    ByteBuffer bBuffer = ByteBuffer.wrap(bArray);
    LongBuffer lBuffer = bBuffer.asLongBuffer();
    lBuffer.put(0, in);
    ID out = new ID();
    out.bytes(bArray);
    return out;
  }
  
  /**
   * Verify the equality of ID objects. Both being null references is
   * considered equal.
   */
  public static boolean IDsEqual(ID a, ID b) {
    if (a == null && b == null) { return true; }
    if (a == null || b == null) { return false; }
    
    byte[] aBytes = a.bytes();
    byte[] bBytes = b.bytes();
    
    if (aBytes.length != bBytes.length) { return false; }
    if (aBytes == null && bBytes == null) { return true; }
    if (aBytes == null || bBytes == null) { return false; }
    
    for (int i = 0; i < aBytes.length; i++) {
      if (aBytes[i] != bBytes[i]) { return false; }
    }
    return true;
  }
  class TraceResponder implements AvroTrace {
    private SpanStorage spanStorage;
    
    public TraceResponder(SpanStorage spanStorage) {
      this.spanStorage = spanStorage;
    }
  }
  
  private double traceProb; // Probability of starting tracing
  private int port;         // Port to serve tracing data
  private int clientPort;   // Port to expose client HTTP interface
  private StorageType storageType;  // How to store spans
  private long maxSpans; // Max number of spans to store
  private boolean enabled; // Whether to participate in tracing

  private ThreadLocal<Span> currentSpan; // span in which we are server
  private ThreadLocal<Span> childSpan;   // span in which we are client
  
  // Storage and serving of spans
  protected SpanStorage storage;
  protected HttpServer httpServer;
  protected SpecificResponder responder;
  
  // Client interface
  protected Server clientFacingServer;
  
  public TracePlugin(TracePluginConfiguration conf) throws IOException {
    traceProb = conf.traceProb;
    port = conf.port;
    clientPort = conf.clientPort;
    storageType = conf.storageType;
    maxSpans = conf.maxSpans;
    enabled = conf.enabled;
    
    // check bounds
    if (!(traceProb >= 0.0 && traceProb <= 1.0)) { traceProb = 0.0; }
    if (!(port > 0 && port < 65535)) { port = 51001; }
    if (!(clientPort > 0 && clientPort < 65535)) { clientPort = 51200; }
    if (maxSpans < 0) { maxSpans = 5000; }
    
    currentSpan = new ThreadLocal<Span>(){
      @Override protected Span initialValue(){
          return null;
      }
    };
    
    childSpan = new ThreadLocal<Span>(){
      @Override protected Span initialValue(){
          return null;
      }
    };

    if (storageType.equals("MEMORY")) {
      this.storage = new InMemorySpanStorage();
    }
    else { // default
      this.storage = new InMemorySpanStorage();
    }
    
    this.storage.setMaxSpans(maxSpans);
    
    try {
      HOSTNAME = new Utf8(InetAddress.getLocalHost().getHostName());
    } catch (UnknownHostException e) {
      HOSTNAME = new Utf8("unknown");
    }
    
    // Start serving span data
    responder = new SpecificResponder(
        AvroTrace.PROTOCOL, new TraceResponder(this.storage)); 
    httpServer = new HttpServer(responder, this.port);
    
    // Start client-facing servlet
    initializeClientServer();
  }
  
  @Override
  public void clientStartConnect(RPCContext context) {
    // There are two cases in which we will need to seed a trace
    // (1) If we probabilistically decide to seed a new trace
    // (2) If we are part of an existing trace
    
    if ((this.currentSpan.get() == null) && 
        (RANDOM.nextFloat() < this.traceProb) && enabled) {
      // Start new trace
      Span span = createEventlessSpan(null, null, null);
      this.childSpan.set(span);
    }
    
    if ((this.currentSpan.get() != null) && enabled) {
      Span currSpan = this.currentSpan.get();
      Span span = createEventlessSpan(currSpan.traceID, null, currSpan.spanID);   
      this.childSpan.set(span);
    }
    
    if (this.childSpan.get() != null) {
      Span span = this.childSpan.get();
      context.requestHandshakeMeta().put(
          TRACE_ID_KEY, ByteBuffer.wrap(span.traceID.bytes()));
      context.requestHandshakeMeta().put(
          SPAN_ID_KEY, ByteBuffer.wrap(span.spanID.bytes()));
      if (span.parentSpanID != null) {
        context.requestHandshakeMeta().put(
            PARENT_SPAN_ID_KEY, ByteBuffer.wrap(span.parentSpanID.bytes())); 
      }
    }
  }
  
  @Override
  public void serverConnecting(RPCContext context) {
    Map<Utf8, ByteBuffer> meta = context.requestHandshakeMeta();
    // Are we being asked to propagate a trace?
    if (meta.containsKey(TRACE_ID_KEY) && enabled) {
      if (!(meta.containsKey(SPAN_ID_KEY))) {
        LOG.warn("Span ID missing for trace " +
            meta.get(TRACE_ID_KEY).toString());
        return; // should have been given full span data
      }
      byte[] spanIDBytes = new byte[8];
      meta.get(SPAN_ID_KEY).get(spanIDBytes);
      ID spanID = new ID();
      spanID.bytes(spanIDBytes);
      
      ID parentSpanID = null;
      if (meta.get(PARENT_SPAN_ID_KEY) != null) {
        parentSpanID = new ID();
        parentSpanID.bytes(meta.get(PARENT_SPAN_ID_KEY).array());
      }
      ID traceID = new ID();
      traceID.bytes(meta.get(TRACE_ID_KEY).array());
      
      Span span = createEventlessSpan(traceID, spanID, parentSpanID);
      
      span.events = new GenericData.Array<TimestampedEvent>(
          100, Schema.createArray(TimestampedEvent.SCHEMA$));
      this.currentSpan.set(span);
    }
  }
  
  @Override
  public void clientFinishConnect(RPCContext context) { }

  @Override
  public void clientSendRequest(RPCContext context) {
    if (this.childSpan.get() != null) {
      Span child = this.childSpan.get();
      addEvent(child, SpanEvent.CLIENT_SEND);
      child.messageName = new Utf8(
          context.getMessage().getName());
      child.requestPayloadSize = getPayloadSize(context.getRequestPayload());
    }
  }
 
  @Override
  public void serverReceiveRequest(RPCContext context) {
    if (this.currentSpan.get() != null) {
      Span current = this.currentSpan.get();
      addEvent(current, SpanEvent.SERVER_RECV);
      current.messageName = new Utf8(
          context.getMessage().getName());
      current.requestPayloadSize = getPayloadSize(context.getRequestPayload());
    }
  }
  
  @Override
  public void serverSendResponse(RPCContext context) {
    if (this.currentSpan.get() != null) {
      Span current = this.currentSpan.get();
      addEvent(current, SpanEvent.SERVER_SEND);
      current.responsePayloadSize = 
        getPayloadSize(context.getResponsePayload());
      this.storage.addSpan(this.currentSpan.get());
      this.currentSpan.set(null);
    }
  }
  
  @Override
  public void clientReceiveResponse(RPCContext context) {
    if (this.childSpan.get() != null) {
      Span child = this.childSpan.get();
      addEvent(child, SpanEvent.CLIENT_RECV);
      child.responsePayloadSize = 
        getPayloadSize(context.getResponsePayload());
      this.storage.addSpan(this.childSpan.get());
      this.childSpan.set(null);
    }
  }
  
  /**
   * Start a client-facing server. Can be overridden if users
   * prefer to attach client Servlet to their own server. 
   */
  protected void initializeClientServer() {
    clientFacingServer = new Server(clientPort);
    Context context = new Context(clientFacingServer, "/");
    context.addServlet(new ServletHolder(new TraceClientServlet()), "/");
    try {
      clientFacingServer.start();
    } catch (Exception e) {
      
    }
  }
}
