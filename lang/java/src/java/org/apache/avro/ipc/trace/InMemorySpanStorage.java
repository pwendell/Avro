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
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.ipc.ByteBufferOutputStream;
import org.apache.avro.specific.SpecificDatumWriter;

/**
 * Example implementation of SpanStorage which keeps spans in memory.
 * 
 * This is designed as a prototype for demonstration and testing. It should only 
 * be used in production settings if very small amount of tracing data
 * is being recorded.
 *
 */
public class InMemorySpanStorage implements SpanStorage {
  private static long DEFAULT_MAX_SPANS = 10000;
  
  protected LinkedList<Span> spans;
  protected HashMap<Long, Query> queries;
  private long maxSpans;
  private long lastQueryHandle = 0;
  
  public InMemorySpanStorage() {
    this.spans = new LinkedList<Span>();
    this.maxSpans = DEFAULT_MAX_SPANS;
  }
  
  @Override
  public void addSpan(Span s) {
    this.spans.add(s);
    if (this.spans.size() > this.maxSpans) {
      this.spans.removeFirst();
    }
  }

  @Override
  public long getQueryHandle(Query q) {
    queries.put(lastQueryHandle + 1, q);
    lastQueryHandle = lastQueryHandle + 1;
    return lastQueryHandle;
  }

  @SuppressWarnings("unchecked")
  @Override
  public ByteBuffer getSpanBlock(long queryHandle) throws IOException {
    ByteBufferOutputStream bbo = new ByteBufferOutputStream();
    BinaryEncoder encoder = new BinaryEncoder(bbo);
    SpecificDatumWriter writer = new SpecificDatumWriter(Span.SCHEMA$);
    
    for (Span s : spans) {
      writer.write(s, encoder);
    }
    
    List<ByteBuffer> buffers = bbo.getBufferList();
    
    int totalBytes = 0;
    for (ByteBuffer bb : buffers) {
      totalBytes += bb.position();
    }
    
    ByteBuffer out = ByteBuffer.allocate(totalBytes);
    for (ByteBuffer bb : buffers) {
      out.put(bb);
    }
    
    return out;
  }

  @Override
  public void destroyQueryHandle(long queryHandle) {
    if (queries.containsKey(queryHandle)) {
      queries.remove(queryHandle);
    }
  }

  @Override
  public void setMaxSpans(long maxSpans) {
    this.maxSpans = maxSpans;
    while (this.spans.size() > maxSpans) {
      this.spans.removeFirst();
    }
  }

  @Override
  public List<Span> getAllSpans() {
    return this.spans;
  }
}
