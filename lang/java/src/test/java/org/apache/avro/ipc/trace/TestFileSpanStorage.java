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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.apache.avro.util.Utf8;
import org.junit.Test;

/**
 * Unit tests for { @link FileSpanStorage }.
 */
public class TestFileSpanStorage {
  
  @Test
  public void testBasicStorage() {
    SpanStorage test = new FileSpanStorage(false);
    Span s = Util.createEventlessSpan(Util.IDValue(1), Util.IDValue(1), null);
    s.messageName = new Utf8("message");
    test.addSpan(s);
    try {
      Thread.sleep(100);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    assertTrue(test.getAllSpans().contains(s));
  }
  
  @Test
  public void testTonsOfSpans() {
    SpanStorage test = new FileSpanStorage(false);
    test.setMaxSpans(100000);
    List<Span> spans = new ArrayList<Span>(50000);
    for (int i = 0; i < 50000; i++) {
      Span s = Util.createEventlessSpan(Util.IDValue(i), Util.IDValue(i), null);
      s.messageName = new Utf8("message");
      test.addSpan(s);
      spans.add(s);
    }
    try {
      Thread.sleep(100);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    assertEquals(50000, test.getAllSpans().size());
    
    // Test fewer spans but explicitly call containsAll
    SpanStorage test2 = new FileSpanStorage(false);
    test.setMaxSpans(100000);
    spans.clear();
    for (int i = 0; i < 5000; i++) {
      Span s = Util.createEventlessSpan(Util.IDValue(i), Util.IDValue(i), null);
      s.messageName = new Utf8("message");
      test2.addSpan(s);
      spans.add(s);
    }
    try {
      Thread.sleep(100);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    assertTrue(test.getAllSpans().containsAll(spans));
    
  }
  
  @Test
  public void testBasicMaxSpans() {
    SpanStorage test = new FileSpanStorage(false);
    test.setMaxSpans(10);
    
    // Add a bunch of spans
    for (int i = 0; i < 100; i++) {
      Span s = Util.createEventlessSpan(Util.IDValue(i), Util.IDValue(i), null);
      s.messageName = new Utf8("message");
      test.addSpan(s);
    }
    
    List<Span> lastNine = new LinkedList<Span>();
    for (int i = 0; i < 9; i++) {
      Span s = Util.createEventlessSpan(Util.IDValue(100 + i), Util.IDValue(100 + i), null);
      s.messageName = new Utf8("message");
      lastNine.add(s);
      test.addSpan(s);
    }
    try {
      Thread.sleep(100);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    List<Span> retreived = test.getAllSpans();
    assertTrue(retreived.size() == 9);
    assertTrue(retreived.containsAll(lastNine));
  }
}