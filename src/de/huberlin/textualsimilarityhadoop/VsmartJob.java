package de.huberlin.textualsimilarityhadoop;

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
public class VsmartJob extends Configured implements Tool {

  @Override
  public int run(String[] args) throws Exception {
    System.out.println("VSmart");
    ParameterParser pp = new ParameterParser(args);
    
    Configuration conf = this.getConf();
    FileSystem filesystem = FileSystem.get(getConf());
    conf.set("delta", pp.getTheta()); // min. textual similarity

    Path inputPath = new Path(pp.getInput());
    String temporaryPathString = pp.getOutput() + "tmp1";
    Path temporaryPath = new Path(temporaryPathString);
    filesystem.delete(temporaryPath, true);
    String outputPathString = pp.getOutput();
    Path outputPath = new Path(outputPathString);
    filesystem.delete(outputPath, true);
    
    System.out.println("Step 1: Replicate by token and create candidate pairs");
    Job job1a = Job.getInstance(conf, "Step 1: Replicate by token and create candidate pairs");
    job1a.setJarByClass(VsmartReplicationJob.class);
    
    job1a.setMapperClass(VsmartReplicationJob.ReplicationMapper.class);
    job1a.setMapOutputKeyClass(IntWritable.class);
    job1a.setMapOutputValueClass(LongWritable.class);
    
    job1a.setReducerClass(VsmartReplicationJob.CandidateReducer.class);
    job1a.setOutputKeyClass(BytesWritable.class);
    job1a.setOutputValueClass(BytesWritable.class);
    
    FileInputFormat.addInputPath(job1a, inputPath);
//    FileOutputFormat.setOutputPath(job1a, temporaryPath);
    SequenceFileAsBinaryOutputFormat.setOutputPath(job1a, temporaryPath);
    job1a.setOutputFormatClass(SequenceFileAsBinaryOutputFormat.class);
    job1a.waitForCompletion(true);
    
    System.out.println("Step 2: Compute similarity");
    Job job2a = Job.getInstance(conf, "Step 2: Compute similarity");
    job2a.setJarByClass(VsmartSimilarityJob.class);
    
    job2a.setMapperClass(VsmartSimilarityJob.RedirectMapper.class);
    job2a.setMapOutputKeyClass(LongWritable.class);
    job2a.setMapOutputValueClass(IntWritable.class);
    
    job2a.setReducerClass(VsmartSimilarityJob.SimilarityReducer.class);
    
    job2a.setOutputKeyClass(Text.class);
    job2a.setOutputValueClass(NullWritable.class);
    
//    FileInputFormat.addInputPath(job2a, temporaryPath);
    SequenceFileAsBinaryInputFormat.addInputPath(job2a, temporaryPath);
    job2a.setInputFormatClass(SequenceFileAsBinaryInputFormat.class);
    FileOutputFormat.setOutputPath(job2a, outputPath);
    int returnCode = job2a.waitForCompletion(true) ? 0 : 1;

    filesystem.delete(temporaryPath, true);

    return returnCode;
  }
  
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new Configuration(), new VsmartJob(), args);
  }
}
