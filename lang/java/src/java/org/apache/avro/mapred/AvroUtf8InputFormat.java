package org.apache.avro.mapred;
import java.io.IOException;

import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.LineRecordReader;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

/**
 * An {@link org.apache.hadoop.mapred.InputFormat} for text files.
 * Each line is a {@link Utf8} key; values are null.
 */
public class AvroUtf8InputFormat
  extends FileInputFormat<AvroWrapper<Utf8>, NullWritable>
  implements JobConfigurable {

  static class Utf8LineRecordReader implements
    RecordReader<AvroWrapper<Utf8>, NullWritable> {

    private LineRecordReader lineRecordReader;
    
    private LongWritable currentKeyHolder = new LongWritable();
    private Text currentValueHolder = new Text();
    
    public Utf8LineRecordReader(Configuration job, 
        FileSplit split) throws IOException {
      this.lineRecordReader = new LineRecordReader(job, split);
    }
    
    public void close() throws IOException {
      lineRecordReader.close();
    }

    public long getPos() throws IOException {
      return lineRecordReader.getPos();
    }

    public float getProgress() throws IOException {
      return lineRecordReader.getProgress();
    }

    public boolean next(AvroWrapper<Utf8> key, NullWritable value)
      throws IOException {
      boolean success = lineRecordReader.next(currentKeyHolder,
          currentValueHolder);
      if (success) {
        key.datum(new Utf8(currentValueHolder.getBytes())
            .setLength(currentValueHolder.getLength()));
      } else {
        key.datum(null);
      }
      return success;
    }

    @Override
    public AvroWrapper<Utf8> createKey() {
      return new AvroWrapper<Utf8>(null);
    }

    @Override
    public NullWritable createValue() {
      return NullWritable.get();
    }

  }

  private CompressionCodecFactory compressionCodecs = null;

  public void configure(JobConf conf) {
    compressionCodecs = new CompressionCodecFactory(conf);
  }

  protected boolean isSplitable(FileSystem fs, Path file) {
    return compressionCodecs.getCodec(file) == null;
  }

  @Override
  public RecordReader<AvroWrapper<Utf8>, NullWritable>
    getRecordReader(InputSplit split, JobConf job, Reporter reporter)
    throws IOException {

    reporter.setStatus(split.toString());
    return new Utf8LineRecordReader(job, (FileSplit) split);
  }

}
