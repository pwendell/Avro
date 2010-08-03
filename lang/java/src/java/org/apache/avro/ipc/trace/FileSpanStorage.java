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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

/**
 * A file-based { @link SpanStorage } implementation for Avro's 
 * { @link TracePlugin }. This class has two modes, one in which writes are 
 * buffered and one in which they are not. Even without buffering, there will be
 * some delay between reporting of a Span and the actual disk write.
 */
public class FileSpanStorage implements SpanStorage {
  /*
   * We use rolling Avro DataFiles that store Span data associated with ten 
   * minute chunks. Because we enforce an upper limit on the number of spans
   * stored, simply drop oldest file if and when the next write causes us to 
   * exceed that limit.
   * 
   * Focus is on efficiency since most logic occurs every
   * time a span is recorded (that is, every RPC call!).
   * 
   * We never want to block on span adding operations, which occur in the same
   * thread as the Requestor. We are okay to block on span retrieving 
   * operations, since they typically run the RPCPlugin's own servlet. To
   * avoid blocking on span addition we use a separate WriterThread which reads
   * a BlockingQueue of Spans and writes span data to disk.  
   */
  
  private class DiskWriterThread implements Runnable {
    
    /** Shared Span queue. Read-only for this thread. */
    private BlockingQueue<Span> outstanding;
    
    /** Shared queue of files currently in view. Read/write for this thread. */
    private Queue<File> files;
    
    /** How many Spans already written to each file. */
    private HashMap<File, Long> spansPerFile = new HashMap<File, Long>();  
    
    /** Total spans already written so far. */
    private long spansSoFar;
        
    /** DiskWriter for current file. */
    private DataFileWriter<Span> currentWriter;
    
    /** Timestamp of the current file. */
    private Long currentTimestamp = (long) 0;
    
    /** Whether to buffer file writes.*/
    private boolean doBuffer;
    
    /**
     * Thread that runs continuously and writes outstanding requests to
     * Avro files. This thread also deals with rolling files over and dropping
     * old files when the span limit is reached.
     */
    public DiskWriterThread(BlockingQueue<Span> outstanding,  
        Queue<File> files, boolean buffer) {
      this.outstanding = outstanding;
      this.files = files;
      this.doBuffer = buffer;
    }
    
    public void run() {
      while (true) {
        Span s;
        try {
          s = this.outstanding.take();
        } catch (InterruptedException e1) {
          continue;
        }
        try {
          assureCurrentWriter();
          this.currentWriter.append(s);
          if (!this.doBuffer) this.currentWriter.flush();
          this.spansSoFar += 1;
          long fileSpans = this.spansPerFile.get(this.files.peek());
          this.spansPerFile.put(this.files.peek(),fileSpans + 1);
        } catch (IOException e) {
          // Fail silently if can't write
        }
      }
    }
    
    /**
     * Assure that currentWriter is populated and refers to the correct
     * data file. This may roll-over the existing data file. Also assures
     * that writing one more span will not violate limits on Span storage.
     * @throws IOException 
     */
    private void assureCurrentWriter() throws IOException {
      boolean createNewFile = false;
      
      // Will we overshoot policy?
      while (this.spansSoFar >= maxSpans) {
        File oldest = null;
        synchronized (this.files) {
          oldest = this.files.poll();
        }
        if (oldest == null) {
          throw new RuntimeException("Bad state.");
        }
        this.spansSoFar -= spansPerFile.get(oldest);
        spansPerFile.remove(oldest);
        oldest.delete();
      }
      if (files.size() == 0) { 
        // In corner case we have removed the current file,
        // if that happened we need to clear current variables.
        currentTimestamp = (long) 0;
        currentWriter = null;
      }
      
      long currentStamp = System.currentTimeMillis() / 1000L;
      long cutOff = currentStamp - (currentStamp % SECONDS_PER_FILE);
      if (currentWriter == null) {
        createNewFile = true;
      }
      else if (cutOff > (currentTimestamp + SECONDS_PER_FILE)) {
        currentWriter.close();
        createNewFile = true;
      }
      if (createNewFile) {
        File newFile = new File(TRACE_FILE_DIR + "/" + 
          Thread.currentThread().getId() + "_" + currentStamp + FILE_SUFFIX);
        synchronized (this.files) {
          this.files.add(newFile);
        }
        this.spansPerFile.put(newFile, (long) 0);
        this.currentWriter = new DataFileWriter<Span>(SPAN_WRITER);
        this.currentWriter.setCodec(CodecFactory.deflateCodec(9));
        this.currentWriter.create(Span.SCHEMA$, newFile);
        currentTimestamp = cutOff;
      }
    }
  }
  
  /** Granularity of file chunks. */
  private final static int SECONDS_PER_FILE = 60 * 10; // ten minute chunks
  
  /** Directory of data files */
  private static String TRACE_FILE_DIR = "/tmp";
  private final static String FILE_SUFFIX = ".av";

  private final static SpecificDatumWriter<Span> SPAN_WRITER = 
    new SpecificDatumWriter<Span>(Span.class);
  private final static SpecificDatumReader<Span> SPAN_READER = 
    new SpecificDatumReader<Span>(Span.class);
  
  /** How frequently to poll for new Spans, in Milliseconds */
  public final static long POLL_FREQUNECY_MILLIS = 50;
  
  private long maxSpans = DEFAULT_MAX_SPANS;

  /** Shared queue of files currently in view. This thread only reads.*/
  private Queue<File> files = new LinkedList<File>();
  
  /** Shared Span queue. This thread only writes. */
  LinkedBlockingQueue<Span> outstanding = new LinkedBlockingQueue<Span>();
  
  /** DiskWriter thread */
  Thread writer;
  Boolean writerEnabled;
  
  public FileSpanStorage(boolean buffer) {
    this.writerEnabled = true;
    this.writer = new Thread(new DiskWriterThread(outstanding, files, buffer));
    this.writer.start();
  }
  
 protected void finalize() {
    this.writerEnabled = false;
  }
  
  @Override
  public void addSpan(Span s) {
    this.outstanding.add(s);
  }

  @Override
  public List<Span> getAllSpans() {
    ArrayList<Span> out = new ArrayList<Span>();
    synchronized (this.files) { 
      for (File f: this.files) {
        DataFileReader<Span> reader;
        try {
          reader = new DataFileReader<Span>(f, SPAN_READER);
        } catch (IOException e) {
          continue; // Skip if there is a problem with this file
        }
        Iterator<Span> it = reader.iterator();
        ArrayList<Span> spans = new ArrayList<Span>();
        while (it.hasNext()) {
          spans.add(it.next());
        }
        out.addAll(spans);
      }
    }
    return out;
  }

  @Override
  public void setMaxSpans(long maxSpans) {
    this.maxSpans = maxSpans;
  }
}
