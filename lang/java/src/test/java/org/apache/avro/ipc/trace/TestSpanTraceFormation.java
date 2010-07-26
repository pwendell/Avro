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

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.apache.avro.util.Utf8;
import org.junit.Test;

public class TestSpanTraceFormation {
  
  @Test
  public void testSpanEquality() {
    Span root = new Span();
    root.spanID = TracePlugin.IDValue(10);
    root.parentSpanID = null;
    root.messageName = new Utf8("startCall");
    
    Span a = new Span();
    a.spanID = TracePlugin.IDValue(11);
    a.parentSpanID = TracePlugin.IDValue(10);
    a.messageName = new Utf8("childCall1");
    
    Span b = new Span();
    b.spanID = TracePlugin.IDValue(12);
    b.parentSpanID = TracePlugin.IDValue(10);
    b.messageName = new Utf8("childCall2");
    
    Span c = new Span();
    c.spanID = TracePlugin.IDValue(13);
    c.parentSpanID = TracePlugin.IDValue(10);
    c.messageName = new Utf8("childCall3");
    
    List<Span> spans = new LinkedList<Span>();
    spans.add(root);
    spans.add(a);
    spans.add(b);
    spans.add(c);
    Trace trace1 = Trace.extractTrace(spans);
    
    Span d = new Span();
    d.spanID = TracePlugin.IDValue(11);
    d.parentSpanID = TracePlugin.IDValue(10);
    d.messageName = new Utf8("childCall1");
    
    Span e = new Span();
    e.spanID = TracePlugin.IDValue(12);
    e.parentSpanID = TracePlugin.IDValue(10);
    e.messageName = new Utf8("childCall2");
    
    Span f = new Span();
    f.spanID = TracePlugin.IDValue(13);
    f.parentSpanID = TracePlugin.IDValue(10);
    f.messageName = new Utf8("childCall3");
    
    spans.clear();
    spans.add(d);
    spans.add(e);
    spans.add(f);
    spans.add(root);
    Trace trace2 = Trace.extractTrace(spans);
    
    assertEquals(trace1.executionPathHash(), trace2.executionPathHash());
  }
  
  
  @Test
  public void testSpanEquality2() {
    Span root = new Span();
    root.spanID = TracePlugin.IDValue(10);
    root.parentSpanID = null;
    root.messageName = new Utf8("startCall");
    
    Span a = new Span();
    a.spanID = TracePlugin.IDValue(11);
    a.parentSpanID = TracePlugin.IDValue(10);
    a.messageName = new Utf8("childCall1");
    
    Span b = new Span();
    b.spanID = TracePlugin.IDValue(12);
    b.parentSpanID = TracePlugin.IDValue(10);
    b.messageName = new Utf8("childCall2");
    
    Span c = new Span();
    c.spanID = TracePlugin.IDValue(13);
    c.parentSpanID = TracePlugin.IDValue(10);
    c.messageName = new Utf8("childCall3");
    
    List<Span> spans = new LinkedList<Span>();
    spans.add(root);
    spans.add(a);
    spans.add(b);
    spans.add(c);
    Trace trace1 = Trace.extractTrace(spans);
    
    Span d = new Span();
    d.spanID = TracePlugin.IDValue(11);
    d.parentSpanID = TracePlugin.IDValue(10);
    d.messageName = new Utf8("childCall1");
    
    Span e = new Span();
    e.spanID = TracePlugin.IDValue(12);
    e.parentSpanID = TracePlugin.IDValue(10);
    e.messageName = new Utf8("childCall2");
    
    Span f = new Span();
    f.spanID = TracePlugin.IDValue(13);
    f.parentSpanID = TracePlugin.IDValue(10);
    f.messageName = new Utf8("childCall3");
    
    Span g = new Span();
    g.spanID = TracePlugin.IDValue(14);
    g.parentSpanID = TracePlugin.IDValue(13);
    g.messageName = new Utf8("childCall4");
    
    spans.clear();
    spans.add(d);
    spans.add(e);
    spans.add(f);
    spans.add(g);
    spans.add(root);
    Trace trace2 = Trace.extractTrace(spans);
    
    assertFalse(trace1.executionPathHash() == trace2.executionPathHash());
  }
  
  /**
   * Describe this trace with two different orderings and make sure
   * they equate to being equal. 
   * 
   *    b
   *   / 
   *  a -b    g-i
   *   \     /
   *    d-e-f
   *        \
   *         g-i
   */
 
  @Test
  public void testSpanEquality3() {
    
    Span a = new Span();
    a.spanID = TracePlugin.IDValue(1);
    a.parentSpanID = null;
    a.messageName = new Utf8("a");
    
    Span b1 = new Span();
    b1.spanID = TracePlugin.IDValue(2);
    b1.parentSpanID = TracePlugin.IDValue(1);
    b1.messageName = new Utf8("b");
    
    Span b2 = new Span();
    b2.spanID = TracePlugin.IDValue(3);
    b2.parentSpanID = TracePlugin.IDValue(1);
    b2.messageName = new Utf8("b");
    
    Span d = new Span();
    d.spanID = TracePlugin.IDValue(4);
    d.parentSpanID = TracePlugin.IDValue(1);
    d.messageName = new Utf8("d");
    
    Span e = new Span();
    e.spanID = TracePlugin.IDValue(5);
    e.parentSpanID = TracePlugin.IDValue(4);
    e.messageName = new Utf8("e");
    
    Span f = new Span();
    f.spanID = TracePlugin.IDValue(6);
    f.parentSpanID = TracePlugin.IDValue(5);
    f.messageName = new Utf8("f");
    
    Span g1 = new Span();
    g1.spanID = TracePlugin.IDValue(7);
    g1.parentSpanID = TracePlugin.IDValue(6);
    g1.messageName = new Utf8("g");
    
    Span g2 = new Span();
    g2.spanID = TracePlugin.IDValue(8);
    g2.parentSpanID = TracePlugin.IDValue(6);
    g2.messageName = new Utf8("g");
    
    Span i1 = new Span();
    i1.spanID = TracePlugin.IDValue(9);
    i1.parentSpanID = TracePlugin.IDValue(7);
    i1.messageName = new Utf8("i");
    
    Span i2 = new Span();
    i2.spanID = TracePlugin.IDValue(10);
    i2.parentSpanID = TracePlugin.IDValue(8);
    i2.messageName = new Utf8("i");
    
    List<Span> spans = new LinkedList<Span>();
    spans.addAll(Arrays.asList(new Span[] {a, b1, b2, d, e, f, g1, g2, i1, i2}));
    Trace trace1 = Trace.extractTrace(spans);
    
    // Re-order and make sure still equivalent
    spans.clear();
    spans.addAll(Arrays.asList(new Span[] {i2, b1, g1, e, f, d, b2, g2, i1, a}));
    Trace trace2 = Trace.extractTrace(spans);
    assertNotNull(trace1);
    assertNotNull(trace2);
    assertEquals(trace1.executionPathHash(), trace2.executionPathHash());
    assertEquals(trace1.executionPathHash(), trace2.executionPathHash());
    
    // Remove a span and make sure not equivalent
    spans.clear();
    spans.addAll(Arrays.asList(new Span[] {i2, b1, g1, e, f, d, b2, g2, a}));
    Trace trace3 = Trace.extractTrace(spans);
    assertNotNull(trace3);
    assertTrue(trace1.executionPathHash() == trace2.executionPathHash());
    assertTrue(trace1.executionPathHash() != trace3.executionPathHash());
    assertTrue(trace2.executionPathHash() != trace3.executionPathHash());
  }
  
  @Test
  public void testBasicTraceFormation() {
    Span root = new Span();
    root.spanID = TracePlugin.IDValue(10);
    root.parentSpanID = null;
    root.messageName = new Utf8("startCall");
    
    Span a = new Span();
    a.spanID = TracePlugin.IDValue(11);
    a.parentSpanID = TracePlugin.IDValue(10);
    a.messageName = new Utf8("childCall1");
    
    Span b = new Span();
    b.spanID = TracePlugin.IDValue(12);
    b.parentSpanID = TracePlugin.IDValue(10);
    b.messageName = new Utf8("childCall2");
    
    Span c = new Span();
    c.spanID = TracePlugin.IDValue(13);
    c.parentSpanID = TracePlugin.IDValue(10);
    c.messageName = new Utf8("childCall3");
    
    List<Span> spans = new LinkedList<Span>();
    spans.add(root);
    spans.add(a);
    spans.add(b);
    spans.add(c);
    Trace trace = Trace.extractTrace(spans);
    
    assertNotNull(trace);
    
    TraceNode rootNode = trace.getRoot();
    assertEquals(rootNode.span.messageName, new Utf8("startCall"));
    assertEquals(3, rootNode.children.size());
    boolean found1, found2, found3;
    found1 = found2 = found3 = false;
    
    for (TraceNode tn: rootNode.children) {
      if (tn.span.messageName.equals(new Utf8("childCall1"))) {
        found1 = true;
      }
      if (tn.span.messageName.equals(new Utf8("childCall2"))) {
        found2 = true;
      }
      if (tn.span.messageName.equals(new Utf8("childCall3"))) {
        found3 = true;
      }
      assertNotNull(tn.children);
      assertEquals(0, tn.children.size());
    }
    assertTrue(found1);
    assertTrue(found2);
    assertTrue(found3);
   }
 }
