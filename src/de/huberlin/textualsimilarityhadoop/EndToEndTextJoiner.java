package de.huberlin.textualsimilarityhadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class EndToEndTextJoiner {

  /**
   * input: 
   * similar pairs of rids: {<rid1 rid2>}
   * original input augmented with rids: {<rid tokenInt,tokenInt,... originalText>}
   * 
   */
  public static class EndMapper1 extends Mapper<Object, Text, IntWritable, Text> {
    private final IntWritable outKey = new IntWritable();
    private final Text outValue = new Text();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String[] valueArr = value.toString().split("\\s+");
      if (valueArr.length == 2) { // rid pair:
        outKey.set(Integer.parseInt(valueArr[0]));
        outValue.set(valueArr[1]);
        context.write(outKey, outValue);
        
        outKey.set(Integer.parseInt(valueArr[1]));
        outValue.set(valueArr[0]);
        context.write(outKey, outValue);
      } else { // original input:
        outKey.set(Integer.parseInt(valueArr[0]));
        String origText = "";
        for (int i = 2; i < valueArr.length; i++) {
          origText += valueArr[i];
        }
        
        outValue.set(" " + origText);
        context.write(outKey, outValue);
      }  
    }
  }
  
  public static class EndReducer1 extends Reducer<IntWritable, Text, Text, NullWritable> {
    private final Text outKey = new Text();
    private final NullWritable outVal = NullWritable.get();

    @Override
    public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      Iterator<Text> it = values.iterator();
      String originalText = "";
      ArrayList<Integer> ridList = new ArrayList();
      while (it.hasNext()) {
        String currentElt = it.next().toString();
        // find out if this is a pure text or an rid:
        if (currentElt.startsWith(" ")) { // original record:
          originalText = currentElt;
        } else { // rid pair
          ridList.add(Integer.parseInt(currentElt));
        }
      }
      
      int rid0 = key.get();
      for (Integer rid1 : ridList) {
        if (rid0 < rid1) {
          outKey.set(rid0 + "_" + rid1 + " " + originalText);
        } else {
          outKey.set(rid1 + "_" + rid0 + " " + originalText);
        }
        
        context.write(outKey, outVal);
      }

    }
  }

  
  
  
  public static class EndMapper2 extends Mapper<Object, Text, Text, Text> {
    private final Text outKey = new Text();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String[] valueArr = value.toString().split("\\s+");

      outKey.set(valueArr[0]);
      context.write(outKey, value);
    }
  }

  
  public static class EndReducer2 extends Reducer<Text, Text, Text, NullWritable> {
    private final Text outKey = new Text();
    private final NullWritable outValue = NullWritable.get();

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      Iterator<Text> it = values.iterator();
      
      String text0 = it.next().toString();
      String text1 = it.next().toString();
      
      outKey.set(text0 + " " + text1);
      context.write(outKey, outValue);
    }
  }
}
