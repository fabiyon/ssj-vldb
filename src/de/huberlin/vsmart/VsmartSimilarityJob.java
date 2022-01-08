package de.huberlin.vsmart;

import de.huberlin.textualsimilarityhadoop.*;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @author fabi
 */
public class VsmartSimilarityJob extends Configured implements Tool {

  @Override
  public int run(String[] args) throws Exception {
    System.out.println("VSmart");
    ParameterParser pp = new ParameterParser(args);
    
    Configuration conf = this.getConf();
    FileSystem filesystem = FileSystem.get(getConf());
    conf.set("delta", pp.getTheta()); // min. textual similarity

    Path temporaryPath = new Path(pp.getOutput() + VsmartJoinDriver.JOINPARTITIONS_SUFFIX);
    String outputPathString = pp.getOutput();
    Path outputPath = new Path(outputPathString);
    filesystem.delete(outputPath, true);
    
    System.out.println("VSmart: Step 2: Compute similarity");
    Job job2a = Job.getInstance(conf, "Step 2: Compute similarity");
    job2a.setJarByClass(VsmartSimilarityJob.class);
    
    job2a.setMapperClass(RedirectMapper.class);
    job2a.setMapOutputKeyClass(LongWritable.class);
    job2a.setMapOutputValueClass(IntWritable.class);
    job2a.setCombinerClass(SimilarityCombiner.class);
    
    job2a.setReducerClass(SimilarityReducer.class);
    job2a.setOutputKeyClass(NullWritable.class);
    job2a.setOutputValueClass(Text.class);
    
    FileInputFormat.addInputPath(job2a, temporaryPath);

    FileOutputFormat.setOutputPath(job2a, outputPath);
    return job2a.waitForCompletion(true) ? 0 : 1;
  }
  
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new Configuration(), new VsmartSimilarityJob(), args);
  }
  
  
  public static class RedirectMapper extends Mapper<LongWritable, Text, LongWritable, IntWritable> {
    private final LongWritable outKey = new LongWritable();
    private final IntWritable outVal = new IntWritable(1);
    long tmp1;
    long tmp2;
    int rid1;
    int rid2;
    int length1;
    int length2;
    
    private void write(int rid1, int rid2, int sumOfLengths, Context context) throws IOException, InterruptedException {
      outVal.set(sumOfLengths);
      if (rid1 > rid2) {
        int tmp = rid1;
        rid1 = rid2;
        rid2 = tmp;
      }
      long c = ((long)rid1 << 32) | ((long)rid2 & 0xFFFFFFFL);
      outKey.set(c);
      context.write(outKey, outVal);
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      String[] text = value.toString().split("\\s+");
      if (text.length == 1) { // chunk to join with itself
        HashMap<Integer, Integer> buffer = new HashMap();
        for (String left : text[0].split(",")) {
          tmp1 = Long.parseLong(left);
          rid1 = (int)(tmp1 >> 32);
          length1 = (int)tmp1;
          
          for(Map.Entry<Integer,Integer> entry : buffer.entrySet()) {
            write(rid1, entry.getKey(), (entry.getValue() + length1), context);
          }
          
          buffer.put(rid1, length1);
        }        
      } else if (text.length == 2) { // 2 chunks to join with each other
        HashMap<Integer, Integer> leftBuffer = new HashMap();
        for (String left : text[0].split(",")) {
          tmp1 = Long.parseLong(left);
          rid1 = (int)(tmp1 >> 32);
          length1 = (int)tmp1;
          leftBuffer.put(rid1, length1);
        }
        
        for (String right : text[1].split(",")) {
          tmp2 = Long.parseLong(right);
          rid2 = (int)(tmp2 >> 32);
          length2 = (int)tmp2;
          for(Map.Entry<Integer,Integer> entry : leftBuffer.entrySet()) {
            write(entry.getKey(), rid2, (entry.getValue() + length2), context);
          }
        }
        
      } else { // rid1 rid2 sumOfLengths
        rid1 = Integer.parseInt(text[0]);
        rid2 = Integer.parseInt(text[1]);
        write(rid1, rid2, Integer.parseInt(text[2]), context);
      }
    }
  }
  
  public static class SimilarityCombiner extends Reducer<LongWritable, IntWritable, LongWritable, IntWritable> {

    @Override
    public void reduce(LongWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      Iterator<IntWritable> it = values.iterator();
      context.write(key, it.next());
    }
  }
  
  public static class SimilarityReducer extends Reducer<LongWritable, IntWritable, NullWritable, Text> {
    private final Text outVal = new Text();
    private final NullWritable outKey = NullWritable.get();
    private double delta;
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      delta = Double.parseDouble(context.getConfiguration().get("delta"));
    }

    @Override
    public void reduce(LongWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
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
        
        outVal.set(rid1 + " " + rid2);
        context.write(outKey, outVal);
      }
    }
  }
}
