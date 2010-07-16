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
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;


import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.ipc.ByteBufferOutputStream;
import org.apache.avro.ipc.RPCContext;
import org.apache.avro.ipc.RPCPlugin;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;

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
 * 
 * Configuration options are as follows. None are required:
 *   NAME               TYPE    MIN   MAX      DEFAULT
 *   traceProbability   float   0.0   1.0      0.0       // trace frequency
 *   port               int     0     65535    51001     // port to serve traces
 *   maxSpans           long    0     inf      5000      // max traces stored
 *   storageType        string                 MEMORY    // how to store traces
 * @author patrick
 *
 */
public class TracePlugin extends RPCPlugin {
  /*
   * Our base unit of tracing is a Span, which is uniquely described by a
   * span id.  
   */
  private static DecoderFactory FACTORY;
  private static BinaryDecoder DECODER;
  private static BinaryEncoder ENCODER;
  private static Random RANDOM;
  
  // Keys used for Avro meta-data
  private static final Utf8 TRACE_ID_KEY = new Utf8("traceID");
  private static final Utf8 SPAN_ID_KEY = new Utf8("spanID");
  private static final Utf8 PARENT_SPAN_ID_KEY = new Utf8("parentSpanID");
  
  // Helper function for encoding meta-data values
  private static Long decodeLong(ByteBuffer data) {
    DECODER = FACTORY.createBinaryDecoder(data.array(), null);
    try {
      return DECODER.readLong();
    } catch (IOException e) {
      return new Long(-1);
    }
  }
  private static ByteBuffer encodeLong(Long data) throws IOException {
    ByteBufferOutputStream bbOutStream = new ByteBufferOutputStream();
    ENCODER = new BinaryEncoder(bbOutStream);
    ENCODER.writeLong(data);
    return bbOutStream.getBufferList().get(0);
  }
  
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
  
  // Add a timestampted event to this span
  private static void addEvent(Span span, SpanEventType eventType) {
    TimestampedEvent ev = new TimestampedEvent();
    ev.event = eventType;
    ev.timeStamp = System.currentTimeMillis();
    span.events.add(ev);
  }
  
  private float traceProb; // Probability of starting tracing
  private int port;        // Port to serve tracing data
  private String storageType;  // How to store spans
  private long maxSpans; // Max number of spans to store

  private ThreadLocal<Span> currentSpan; // span in which we are server
  private ThreadLocal<Span> childSpan; // span in which we are client
  
  protected SpanStorage storage;

  public TracePlugin(Configuration conf) {
    traceProb = conf.getFloat("traceProbability", (float) 0.0);
    port = conf.getInt("port", 51001);
    storageType = conf.getStrings("spanStorage", "MEMORY")[0];
    maxSpans = conf.getLong("maxSpans", 5000);
    
    // check bounds
    if (!(traceProb >= 0.0 && traceProb <= 1.0)) { traceProb = (float) 0.0; }
    if (!(port > 0 && port < 65535)) { port = 51001; }
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
    
    FACTORY = new DecoderFactory();
    RANDOM = new Random();

    if (storageType.equals("MEMORY")) {
      this.storage = new InMemorySpanStorage();
    }
    else { // default
      this.storage = new InMemorySpanStorage();
    }
    
    this.storage.setMaxSpans(maxSpans);
  }
  
  @Override
  public void clientStartConnect(RPCContext context) {
    // There are two cases in which we will need to seed a trace
    // (1) If we probabilistically decide to seed a new trace
    // (2) If we are part of an existing trace
    
    if ((this.currentSpan.get() == null) && 
        (RANDOM.nextFloat() < this.traceProb)) {
      Span span = new Span();
      span.spanID = Math.abs(RANDOM.nextLong());
      span.parentSpanID = -1;
      span.traceID = Math.abs(RANDOM.nextLong());
      span.events = new GenericData.Array<TimestampedEvent>(
          100, Schema.createArray(TimestampedEvent.SCHEMA$));
      try {
        span.host = new Utf8(InetAddress.getLocalHost().getHostName());
      } catch (UnknownHostException e1) {
        span.host = new Utf8("Unknown");
      }
      
      this.childSpan.set(span);
    }
    
    if (this.currentSpan.get() != null) {
      Span currSpan = this.currentSpan.get();
      Span span = new Span();
      span.spanID = Math.abs(RANDOM.nextLong());
      span.parentSpanID = currSpan.spanID;
      span.traceID = currSpan.traceID;
      span.events = new GenericData.Array<TimestampedEvent>(
          100, Schema.createArray(TimestampedEvent.SCHEMA$));
      try {
        span.host = new Utf8(InetAddress.getLocalHost().getHostName());
      } catch (UnknownHostException e1) {
        span.host = new Utf8("Unknown");
      }
      
      this.childSpan.set(span);
    }
    
    if (this.childSpan.get() != null) {
      Span span = this.childSpan.get();
      try {
        context.requestHandshakeMeta().put(
            TRACE_ID_KEY, encodeLong(span.traceID));
      } catch (IOException e) {
        return;
      }
      try {
        context.requestHandshakeMeta().put(
            SPAN_ID_KEY, encodeLong(span.spanID));
      } catch (IOException e) {
        return;
      }
      try {
        context.requestHandshakeMeta().put(
            PARENT_SPAN_ID_KEY, encodeLong(span.parentSpanID));
      } catch (IOException e) {
        return;
      }
    }
  }
  
  @Override
  public void serverConnecting(RPCContext context) {
    Map<Utf8, ByteBuffer> meta = context.requestHandshakeMeta();
    // Are we being asked to propagate a trace?
    if (meta.containsKey(TRACE_ID_KEY)) {
      if (!(meta.containsKey(SPAN_ID_KEY) && 
          meta.containsKey(PARENT_SPAN_ID_KEY))) {
        return; // parent should have given full span data
      }
      Span span = new Span();
      span.spanID = decodeLong(meta.get(SPAN_ID_KEY));
      span.parentSpanID = decodeLong(meta.get(PARENT_SPAN_ID_KEY));
      span.traceID = decodeLong(meta.get(TRACE_ID_KEY)); // keep trace id
      span.events = new GenericData.Array<TimestampedEvent>(
          100, Schema.createArray(TimestampedEvent.SCHEMA$));
      try {
        span.host = new Utf8(InetAddress.getLocalHost().getHostName());
      } catch (UnknownHostException e1) {
        span.host = new Utf8("Unknown");
      }
      this.currentSpan.set(span);
    }
  }
  
  @Override
  public void clientFinishConnect(RPCContext context) { }

  @Override
  public void clientSendRequest(RPCContext context) {
    if (this.childSpan.get() != null) {
      Span child = this.childSpan.get();
      addEvent(child, SpanEventType.CLIENT_SEND);
      child.messageName = new Utf8(
          context.getMessage().getName());
      child.requestPayloadSize = getPayloadSize(context.getRequestPayload());
    }
  }
 
  @Override
  public void serverReceiveRequest(RPCContext context) {
    if (this.currentSpan.get() != null) {
      Span current = this.currentSpan.get();
      addEvent(current, SpanEventType.SERVER_RECV);
      current.messageName = new Utf8(
          context.getMessage().getName());
      current.requestPayloadSize = getPayloadSize(context.getRequestPayload());
    }
  }
  
  @Override
  public void serverSendResponse(RPCContext context) {
    if (this.currentSpan.get() != null) {
      Span current = this.currentSpan.get();
      addEvent(current, SpanEventType.SERVER_SEND);
      current.responsePayloadSize = 
        getPayloadSize(context.getResponsePayload());
      this.storage.addSpan(this.currentSpan.get());
      this.currentSpan.set(null);
    }
  }
  
  @Override
  public void clientReceiveResponse(RPCContext context) {
    if (this.childSpan.get() != null) {
      Span child = childSpan.get();
      addEvent(child, SpanEventType.CLIENT_RECV);
      child.responsePayloadSize = 
        getPayloadSize(context.getResponsePayload());
      this.storage.addSpan(this.childSpan.get());
      this.childSpan.set(null);
    }
  }
}
