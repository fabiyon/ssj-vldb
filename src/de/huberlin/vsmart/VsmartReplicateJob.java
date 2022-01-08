package de.huberlin.vsmart;

import de.huberlin.textualsimilarityhadoop.*;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @author fabi
 */
public class VsmartReplicateJob extends Configured implements Tool {

  @Override
  public int run(String[] args) throws Exception {
    ParameterParser pp = new ParameterParser(args);
    
    Configuration conf = this.getConf();
    conf.setInt("maxBufferSize", pp.getMemory());
    FileSystem filesystem = FileSystem.get(getConf());

    Path inputPath = new Path(pp.getInput());
    Path temporaryPath = new Path(pp.getOutput() + VsmartJoinDriver.JOINPARTITIONS_SUFFIX);
    filesystem.delete(temporaryPath, true);
   
    System.out.println("VSmart: Step 1: Replicate by token and create candidate pairs");
    Job job1a = Job.getInstance(conf, "Step 1: Replicate by token and create candidate pairs");
    job1a.setJarByClass(VsmartReplicateJob.class);
    
    job1a.setMapperClass(ReplicationMapper.class);
    job1a.setMapOutputKeyClass(IntWritable.class);
    job1a.setMapOutputValueClass(LongWritable.class);
    
    job1a.setReducerClass(CandidateReducer.class);
    job1a.setOutputKeyClass(NullWritable.class);
    job1a.setOutputValueClass(Text.class);
    
    FileInputFormat.addInputPath(job1a, inputPath);
    FileOutputFormat.setOutputPath(job1a, temporaryPath);

    return job1a.waitForCompletion(true) ? 0 : 1;
  }
  
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new Configuration(), new VsmartReplicateJob(), args);
  }
  
  public static class ReplicationMapper extends Mapper<Object, Text, IntWritable, LongWritable> {
    private final IntWritable outKey = new IntWritable();
    private final LongWritable outValue = new LongWritable();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String[] valueArr = value.toString().split("\\s+");
      if (valueArr.length > 1) { // ignore empty records:
        String[] tokenArr = valueArr[1].split(",");
        long c = (Long.parseLong(valueArr[0]) << 32) | ((long)tokenArr.length & 0xFFFFFFFL); // rid recordCardinality: consolidate both integers in one long
        outValue.set(c);
        for (String token : tokenArr) {
          outKey.set(Integer.parseInt(token));
          context.write(outKey, outValue);
        }    
      }
    }
  }
  
  public static void doLocalJoin(ArrayList<Long> buffer, Context context) throws IOException, InterruptedException {
    NullWritable outKey = NullWritable.get();
    Text outVal = new Text();
    long tmp1;
    long tmp2;
    int rid1;
    int rid2;
    int length1;
    int length2;
    for (int i = 0; i < buffer.size(); i++) {
      tmp1 = buffer.get(i);
      rid1 = (int)(tmp1 >> 32);
      length1 = (int)tmp1;
      for (int j = i + 1; j < buffer.size(); j++) {
        tmp2 = buffer.get(j);
        rid2 = (int)(tmp2 >> 32);
        length2 = (int)tmp2;
        outVal.set(rid1 + " " + rid2 + " " + (length1 + length2));
        context.write(outKey, outVal);
      }
    }
  }
  
  public static class CandidateReducer extends Reducer<IntWritable, LongWritable, NullWritable, Text> {
    private final NullWritable outKey = NullWritable.get();
    private final Text outVal = new Text();
    private int maxBufferSize;
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      maxBufferSize = context.getConfiguration().getInt("maxBufferSize", 100);
    }

    @Override
    public void reduce(IntWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
      Iterator<LongWritable> it = values.iterator(); // rid recordCardinality

      ArrayList<Long> buffer = new ArrayList(); // in case of local join
      ArrayList<String> chunks = new ArrayList(); // in case of chunking to join in the Mapper
      int currentCount = 0;
      boolean doLocalJoin = true;
      String currentChunk = "";
      while (it.hasNext()) {
        if (doLocalJoin) {
          buffer.add(it.next().get());
          currentCount++;
          if (currentCount > maxBufferSize) {
            doLocalJoin = false;
            // consolidate the first chunk:
            for (Long tmp : buffer) {
              if (!currentChunk.isEmpty()) {
                currentChunk += ",";
              }
              currentChunk += tmp; // Das hier sieht lustig aus, weil in dem Long die RID und die Kardinalität des Records codiert ist.
            }
            buffer.clear(); // free memory
            chunks.add(currentChunk);
            currentCount = 0;
            currentChunk = "";
          }
        } else {
          if (!currentChunk.isEmpty()) {
            currentChunk += ",";
          }
          currentChunk += it.next().get();
          currentCount++;
          if (currentCount > maxBufferSize) {
            chunks.add(currentChunk);
            currentCount = 0;
            currentChunk = "";
          }
        }
      }
      
      if (doLocalJoin) {
        doLocalJoin(buffer, context);
      } else {
        // add the last chunk:
        if (!currentChunk.isEmpty()) {
          chunks.add(currentChunk);
        }
        
        // 1. output all chunks to be joined with itself:
        for (String chunk : chunks) {
          outVal.set(chunk);
          context.write(outKey, outVal);
        }
        
        // 2. output all pairs of chunks to be joined with each other:
        for (int i = 0; i < chunks.size(); i++) {
          String o1 = chunks.get(i);
          for (int j = i + 1; j < chunks.size(); j++) {
            String o2 = chunks.get(j);
            outVal.set(o1 + " " + o2);
            context.write(outKey, outVal);
          }
        }
      }
    }
  }
}
