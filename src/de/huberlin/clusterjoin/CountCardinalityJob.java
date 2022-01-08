package de.huberlin.clusterjoin;

import de.huberlin.textualsimilarityhadoop.ParameterParser;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class CountCardinalityJob extends Configured implements Tool {

  public static class CardinalityMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
    IntWritable outKey = new IntWritable(1);

    @Override
    public void map(LongWritable inputKey, Text inputValue, Context context) throws IOException, InterruptedException {
      context.write(outKey, outKey);
    }
  }

  public static class CardinalityReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
    IntWritable outVal = new IntWritable();
    
    @Override
    public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException, NullPointerException {
      Iterator<IntWritable> it = values.iterator();
      int counter = 0;
      while (it.hasNext()) {
        counter += it.next().get();
      }
      outVal.set(counter);
      context.write(key, outVal);
    }
  }
  
  
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new Configuration(), new CountCardinalityJob(), args);
  }

  @Override
  public int run(String[] args) throws Exception {
    System.out.println("ClusterJoin: Cardinality Count");
    ParameterParser pp = new ParameterParser(args);
    String inputPathString = pp.getInput();
    String outputPathString = pp.getOutput() + ClusterJoinDriver.CARDINALITY_SUFFIX;

    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);
    Path inputPath = new Path(inputPathString);
    Path outputPath = new Path(outputPathString);
    fs.delete(outputPath, true);
    
    Job job1 = Job.getInstance(conf, "ClusterJoin Partition Cardinality Estimation Step");
    job1.setJarByClass(CountCardinalityJob.class);

    job1.setMapperClass(CardinalityMapper.class);
    job1.setMapOutputKeyClass(IntWritable.class);
    job1.setMapOutputValueClass(IntWritable.class);
    
    job1.setCombinerClass(CardinalityReducer.class);
    job1.setReducerClass(CardinalityReducer.class);
    job1.setOutputKeyClass(IntWritable.class);
    job1.setOutputValueClass(IntWritable.class);
    job1.setNumReduceTasks(1); // necessary to enforce the same filename
    
    FileInputFormat.addInputPath(job1, inputPath);
    FileOutputFormat.setOutputPath(job1, outputPath);
    
    job1.waitForCompletion(true);
    
    return 0;
  }
  

}
