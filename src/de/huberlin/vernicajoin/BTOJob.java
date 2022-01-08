package de.huberlin.vernicajoin;

import de.huberlin.textualsimilarityhadoop.ParameterParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileAsBinaryInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileAsBinaryOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


/**
 *
 * @author fabi
 */
public class BTOJob extends Configured implements Tool {
  
  @Override
  public int run(String[] args) throws Exception {
    System.out.println("Vernica BTO: Global Token Ordering");
    ParameterParser pp = new ParameterParser(args);
    
    Configuration conf = this.getConf();
    FileSystem filesystem = FileSystem.get(getConf());
    // input: already integer-tokenized records, comma-separated
    String inputPathString = pp.getInput();
    Path inputPath;
//    String tokenizedInputPathString = pp.getOutput() + "Tokenized";
//    Path tokenizedInputPath = new Path(tokenizedInputPathString);
//    if (filesystem.exists(tokenizedInputPath)) { // if there is tokenized input, we take this (end-to-end) <<<<<<<<<<<<<<<
//      inputPath = tokenizedInputPath;
//    } else {
      inputPath = new Path(inputPathString);
//    }
    
    
    String intermediatePathString = pp.getOutput() + "Intermediate";
    Path intermediatePath = new Path(intermediatePathString);
    String tokenOrderPathString = pp.getOutput() + VernicaJoinDriver.TOKENORDER_PATH_SUFFIX;
    Path tokenOrderPathStringPath = new Path(tokenOrderPathString);
    
    filesystem.delete(intermediatePath, true);
    filesystem.delete(tokenOrderPathStringPath, true);
    
    Job job1a = Job.getInstance(conf, "Step 1: Count term frequencies");
    job1a.setJarByClass(VernicaFrequencyJob.class);
    
    job1a.setMapperClass(de.huberlin.vernicajoin.VernicaFrequencyJob.FreqSplitMapper.class);
    job1a.setMapOutputKeyClass(IntWritable.class);
    job1a.setMapOutputValueClass(IntWritable.class);
    
    job1a.setReducerClass(de.huberlin.vernicajoin.VernicaFrequencyJob.FreqCountReducer.class);
    job1a.setCombinerClass(de.huberlin.vernicajoin.VernicaFrequencyJob.FreqCountCombiner.class);
    job1a.setOutputKeyClass(BytesWritable.class);
    job1a.setOutputValueClass(BytesWritable.class);
    
    FileInputFormat.addInputPath(job1a, inputPath);
    SequenceFileAsBinaryOutputFormat.setOutputPath(job1a, intermediatePath);
    job1a.setOutputFormatClass(SequenceFileAsBinaryOutputFormat.class);
    
    job1a.waitForCompletion(true);
    
    Job job2a = Job.getInstance(conf, "Step 2: Assign order");
    job2a.setJarByClass(VernicaFrequencyJob1.class);
    
    job2a.setMapperClass(VernicaFrequencyJob1.SwapMapper.class);
    job2a.setMapOutputKeyClass(BytesWritable.class);
    job2a.setMapOutputValueClass(BytesWritable.class);
    
    job2a.setReducerClass(VernicaFrequencyJob1.AssignReducer.class);
    job2a.setNumReduceTasks(1);
    
    job2a.setOutputKeyClass(IntWritable.class);
    job2a.setOutputValueClass(NullWritable.class);
    
    SequenceFileAsBinaryInputFormat.addInputPath(job2a, intermediatePath);
    job2a.setInputFormatClass(SequenceFileAsBinaryInputFormat.class);
    FileOutputFormat.setOutputPath(job2a, tokenOrderPathStringPath);
    job2a.waitForCompletion(true);
    
    filesystem.delete(intermediatePath, true);
    
    return 0;
  }
  
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new Configuration(), new BTOJob(), args);
  }
  
}
