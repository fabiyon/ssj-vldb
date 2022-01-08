package de.huberlin.vernicajoin;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class VernicaFrequencyJob {

  public static class FreqSplitMapper extends Mapper<Object, Text, IntWritable, IntWritable> {
    private final IntWritable outKey = new IntWritable();
    private final IntWritable outValue = new IntWritable(1);

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String[] valueArr = value.toString().split("\\s+"); // problem: wenn der \\s+ ist funktioniert der Tabulator nicht als Trenner. End-To-End liefert ein Leerzeichen als Trenner
      if (valueArr.length < 2) {
        return;
      }
      String[] tokenArr = valueArr[1].split(",");
      for (String token : tokenArr) {
        outKey.set(Integer.parseInt(token));
        context.write(outKey, outValue);
      }
    }
  }
  
  public static class FreqCountCombiner extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
    private final IntWritable outVal = new IntWritable();

    @Override
    public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {  
      Iterator<IntWritable> it = values.iterator();

      int sum = 0;
      while (it.hasNext()) {
        IntWritable currentText = it.next();
        sum += currentText.get();
      }

      outVal.set(sum);
      context.write(key, outVal);
    }
  }  
  
  public static class FreqCountReducer extends Reducer<IntWritable, IntWritable, BytesWritable, BytesWritable> {
    private final BytesWritable outKey = new BytesWritable();
    private final BytesWritable outVal = new BytesWritable();

    @Override
    public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      Iterator<IntWritable> it = values.iterator();

      int sum = 0;
      while (it.hasNext()) {
        IntWritable currentText = it.next();
        sum += currentText.get();
      }

      byte[] keyBytes = ByteBuffer.allocate(4).putInt(key.get()).array();
      byte[] valBytes = ByteBuffer.allocate(4).putInt(sum).array();
      outKey.set(keyBytes, 0, 4);
      outVal.set(valBytes, 0, 4);

      context.write(outKey, outVal);

    }
  }

}
