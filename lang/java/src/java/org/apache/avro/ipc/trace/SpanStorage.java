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
import java.util.List;

/**
 * Responsible for storing spans locally and answering span queries.
 * 
 * Since query for a given set of spans may persist over several RPC
 * calls, they are indexed by a handle.
 * @author patrick
 *
 */
public interface SpanStorage {
  
  /**
   * Get a handle for the supplied query. This handle should persist until 
   * explicitly destroyed.
   * @param q
   * @return
   */
  public long getQueryHandle(Query q);
  
  /**
   * Destroy any internal reference associated with this query handle.
   * @param queryHandle
   */
  public void destroyQueryHandle(long queryHandle);
  
  /**
   * Return a block of avro-encoded spans which answer this query. If
   * the result set has been exhausted, return an empty ByteBuffer.
   * 
   * @param queryHandle
   * @return
   * @throws IOException
   */
  public ByteBuffer getSpanBlock(long queryHandle) throws IOException;
  
  /**
   * Add a span. 
   * @param s
   */
  public void addSpan(Span s);
  
  /**
   * Set the maximum number of spans to have in storage at any given time.
   * @param bytes
   */
  public void setMaxSpans(long maxSpans);
  
  /**
   * Return a list of all spans currently stored. For testing.
   * @return
   */
  public List<Span> getAllSpans();
}
