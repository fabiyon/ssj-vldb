package de.huberlin.vernicajoin;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class VernicaFrequencyJob1 {

  public static class SwapMapper extends Mapper<BytesWritable, BytesWritable, BytesWritable, BytesWritable> {
    @Override
    public void map(BytesWritable key, BytesWritable value, Context context) throws IOException, InterruptedException {
      context.write(value, key);
    }
  }
  
  public static class AssignReducer extends Reducer<BytesWritable, BytesWritable, IntWritable, IntWritable> {
    private final IntWritable outKey = new IntWritable();
    private final IntWritable outVal = new IntWritable();
    
    @Override
    public void reduce(BytesWritable key, Iterable<BytesWritable> values, Context context) throws IOException, InterruptedException {
      Iterator<BytesWritable> it = values.iterator();
      outVal.set(getIntFromByteArr(key.getBytes(), 0));
      while (it.hasNext()) {
        outKey.set(getIntFromByteArr(it.next().getBytes(), 0));
        context.write(outKey, outVal); // token ID, frequency (the latter one needed for grouping in Massjoin)
      }
    }
  }
  private static Integer getIntFromByteArr(byte[] input, int start) {
    ByteBuffer recordSizeBuffer = ByteBuffer.allocate(4);
    recordSizeBuffer.put(input, start, 4);
    recordSizeBuffer.flip();
    return recordSizeBuffer.getInt();
  }

}
