package de.huberlin.textualsimilarityhadoop;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class VsmartSimilarityJob {

  public static class RedirectMapper extends Mapper<BytesWritable, BytesWritable, LongWritable, IntWritable> {
    private final LongWritable outKey = new LongWritable();
    private final IntWritable outValue = new IntWritable(1);

    @Override
    public void map(BytesWritable key, BytesWritable value, Context context) throws IOException, InterruptedException {
//      String[] valueArr = value.toString().split("\\s+"); // rid1 rid2 sumOfCardinalities
      byte[] compound = value.getBytes();
      ByteBuffer rid1Buffer = ByteBuffer.allocate(4);
      rid1Buffer.put(compound, 0, 4);
      rid1Buffer.flip();
      int rid1 = rid1Buffer.getInt();
      ByteBuffer rid2Buffer = ByteBuffer.allocate(4);
      rid2Buffer.put(compound, 4, 4);
      rid2Buffer.flip();
      int rid2 = rid2Buffer.getInt();
      ByteBuffer sumCardinalitiesBuffer = ByteBuffer.allocate(4);
      sumCardinalitiesBuffer.put(compound, 8, 4);
      sumCardinalitiesBuffer.flip();
      int sumCardinalities = sumCardinalitiesBuffer.getInt();
      
      long c = ((long)rid1 << 32) | ((long)rid2 & 0xFFFFFFFL); // consolidate both integers in one long
      outKey.set(c);
      outValue.set(sumCardinalities);
      context.write(outKey, outValue);
    }
  }
  
  public static class SimilarityReducer extends Reducer<LongWritable, IntWritable, Text, NullWritable> {
    private final Text outKey = new Text();
    private final NullWritable outValue = NullWritable.get();
    private double delta;
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      delta = Double.parseDouble(context.getConfiguration().get("delta"));
    }

    @Override
    public void reduce(LongWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      
      
//      String[] ridPairArr = key.toString().split("_");
      double overlapCount = 0;
      int sumOfCardinalities = 0;
      
      Iterator<IntWritable> it = values.iterator();
      IntWritable currentSumOfCardinalities = null;
      while (it.hasNext()) {
        currentSumOfCardinalities = it.next();
        overlapCount++;
      }
      sumOfCardinalities = currentSumOfCardinalities.get();
      
      double similarity = overlapCount / (sumOfCardinalities - overlapCount);
      
      if (similarity > delta) {
        long combinedKey = key.get();
        int rid1 = (int)(combinedKey >> 32);
        int rid2 = (int)combinedKey;
        
        outKey.set(rid1 + " " + rid2);
        context.write(outKey, outValue);
      }
    }
  }

}
