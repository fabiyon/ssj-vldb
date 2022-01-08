package de.huberlin.textualsimilarityhadoop;

import fs.common.Configure;
import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import fs.generateprefixfile.GeneratePrefixFile;
import fs.generateprefixfile.prefixKey;
import fs.generateprefixfile.prefixKeyGroupingComparator;
import fs.generateprefixfile.prefixKeyPartitioner;
import fs.getridpairs.getRIDPairs;
import fs.partitionandjoin.CompositeKey;
import fs.partitionandjoin.CompositeKeyGroupingComparator;
import fs.partitionandjoin.CompositeKeyPartitioner;
import fs.partitionandjoin.IndexAndPartition;
import fs.partitionandjoin.RidLenPairKey;
import fs.tokenization.TokenizationAndSort;
import java.net.URI;
import java.net.URISyntaxException;

public class FSJoinJob {

  public static void TokenizationAndSortJob(String input, String output)
          throws IOException, InterruptedException, ClassNotFoundException {
    Configuration conf = new Configuration();
//    conf.setBoolean("mapred.output.compress", false);
//    conf.set("mapred.child.java.opts", "-Xmx1024M");
    // myconfigure(conf);
    Job job = new Job(conf, "Tokenization And Sort Job");
    job.setJarByClass(FSJoinJob.class);
    // input outpur format
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    // map reduce class
    job.setMapperClass(TokenizationAndSort.TokenizationAndSortMap.class);
    job.setReducerClass(TokenizationAndSort.TokenizationAndSortReduce.class);
    job.setCombinerClass(TokenizationAndSort.TokenizationAndSortCombiner.class);
    // map
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    // reduce
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    job.setNumReduceTasks(1);

    FileInputFormat.setInputPaths(job, new Path(input));
    FileOutputFormat.setOutputPath(job, new Path(output));
    // Delete output if exists
    FileSystem hdfs = FileSystem.get(conf);
    if (hdfs.exists(new Path(output))) {
      hdfs.delete(new Path(output), true);
    }
    //job.submit();
    job.waitForCompletion(true);
  }

  public static void GeneratePrefixFileJob(String input, String output, double similarityThreshold)
          throws IOException, InterruptedException, ClassNotFoundException, URISyntaxException {
    Configuration conf = new Configuration();
    conf.setDouble("similarityThreshold", similarityThreshold);
//    conf.setBoolean("mapred.output.compress", false);
//    conf.set("mapred.child.java.opts", "-Xmx1024M");
    // myconfigure(conf);
    Job job = new Job(conf, "Generate Prefix File");
    job.setJarByClass(FSJoinJob.class);
    // input outpur format
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    // map reduce class
    job.setMapperClass(GeneratePrefixFile.GeneratePrefixFileMap.class);
    job.setReducerClass(GeneratePrefixFile.GeneratePrefixFileReduce.class);
    // map
    job.setMapOutputKeyClass(prefixKey.class);
    job.setMapOutputValueClass(Text.class);
    // reduce
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    // partitioner and grouper
    job.setPartitionerClass(prefixKeyPartitioner.class);
    job.setGroupingComparatorClass(prefixKeyGroupingComparator.class);

    job.setNumReduceTasks(1);
		//MultipleInputs.addInputPath(conf, path, inputFormatClass, mapperClass);

    FileInputFormat.setInputPaths(job, new Path(input));
    FileOutputFormat.setOutputPath(job, new Path(output));
    if (Configure.RunOnCluster) {
      job.addCacheFile(new URI("fs_dict/part-r-00000#dict"));
    } else {
      DistributedCache.addCacheFile(
            new Path("/tmp/fs_dict/part-r-00000").toUri(), job
            .getConfiguration());
    }
    
    // Delete output if exists
    FileSystem hdfs = FileSystem.get(conf);
    if (hdfs.exists(new Path(output))) {
      hdfs.delete(new Path(output), true);
      System.out.println("delete directory: " + output);
    }
    //job.submit();
    job.waitForCompletion(true);
  }

  public static void IndexAndPartitionJob(String input, String output, double similarityThreshold)
          throws IOException, InterruptedException, ClassNotFoundException, URISyntaxException {
    boolean onlyStatistics = false;
    Configuration conf = new Configuration();
    conf.setDouble("similarityThreshold", similarityThreshold);
//    conf.setBoolean("mapred.output.compress", false);
//    conf.set("mapred.child.java.opts", "-Xmx1024M");
    // myconfigure(conf);
    Job job = new Job(conf, "Index And Partition");
    job.setJarByClass(FSJoinJob.class);
    // input outpur format
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    // map reduce class
    job.setMapperClass(IndexAndPartition.IndexAndPartitionMap.class);
    if (!onlyStatistics) {
      job.setReducerClass(IndexAndPartition.IndexAndPartitionReduce.class);
      // reduce
      job.setOutputKeyClass(RidLenPairKey.class);
      job.setOutputValueClass(IntWritable.class);
    } else {
      job.setReducerClass(IndexAndPartition.IndexAndPartitionStatisticsReduce.class);
      job.setCombinerClass(IndexAndPartition.IndexAndPartitionStatisticsReduce.class);
      // reduce
      job.setOutputKeyClass(CompositeKey.class);
      job.setOutputValueClass(Text.class);
    }
    // map
    job.setMapOutputKeyClass(CompositeKey.class);
    job.setMapOutputValueClass(Text.class);
    
    // partitioner and grouper
    job.setPartitionerClass(CompositeKeyPartitioner.class);
    job.setGroupingComparatorClass(CompositeKeyGroupingComparator.class);

//    job.setNumReduceTasks(5);
    //MultipleInputs.addInputPath(conf, path, inputFormatClass, mapperClass);

    FileInputFormat.setInputPaths(job, new Path(input));
    FileOutputFormat.setOutputPath(job, new Path(output));
    if (Configure.RunOnCluster) {
      job.addCacheFile(new URI("fs_dict/part-r-00000#dict"));
      job.addCacheFile(new URI("fs_prefix/part-r-00000#prefix"));
    } else {
      DistributedCache.addCacheFile(
              new Path("/tmp/fs_dict/part-r-00000").toUri(), job
              .getConfiguration());
      DistributedCache.addCacheFile(
              new Path("/tmp/fs_prefix/part-r-00000").toUri(), job
              .getConfiguration());
    }
    // Delete output if exists
    FileSystem hdfs = FileSystem.get(conf);
    if (hdfs.exists(new Path(output))) {
      hdfs.delete(new Path(output), true);
      System.out.println("delete directory: " + output);
    }
    //job.submit();
    job.waitForCompletion(true);
  }

  public static void getRIDPairsJob(String input, String output, double similarityThreshold) throws IOException, InterruptedException, ClassNotFoundException {
    Configuration conf = new Configuration();
    conf.setDouble("similarityThreshold", similarityThreshold);
//    conf.setBoolean("mapred.output.compress", false);
//    conf.set("mapred.child.java.opts", "-Xmx1024M");
    // myconfigure(conf);
    Job job = new Job(conf, "Perform Join Job");
    job.setJarByClass(FSJoinJob.class);
    // input outpur format
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    //set map and reduce class
    job.setMapperClass(getRIDPairs.RIDPairsMap.class);
    job.setReducerClass(getRIDPairs.RIDPairsReduce.class);
    job.setCombinerClass(getRIDPairs.RIDPairsCombiner.class);
    //set map out
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    // reduce
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

//    job.setNumReduceTasks(5);
    //set the input paths and submit jobs
    FileInputFormat.setInputPaths(job, new Path(input));
    FileOutputFormat.setOutputPath(job, new Path(output));

    // Delete output if exists
    FileSystem hdfs = FileSystem.get(conf);
    if (hdfs.exists(new Path(output))) {
      hdfs.delete(new Path(output), true);
    }
    //job.submit();
    job.waitForCompletion(true);
  }

  public static void main(String[] args) throws IOException,
          InterruptedException, ClassNotFoundException, Exception {
    ParameterParser pp = new ParameterParser(args);
    
//    Configuration conf = this.getConf();
    double similarityThreshold = Double.parseDouble(pp.getTheta());
    String inputPathString = pp.getInput();
    String outputPathString = pp.getOutput();
//    FileSystem filesystem = FileSystem.get(getConf());
//    filesystem.delete(new Path(outputPath), true);
    
    
    String input, output;
    Date startTime = new Date();
    Date endTime;
    //Job 1
    System.out.println("Job started: " + startTime);
    input = inputPathString;
    if (Configure.RunOnCluster) {
      output = "fs_dict";
    } else {
      output = "/tmp/fs_dict";
    }
    
    TokenizationAndSortJob(input, output); // Funktioniert einwandfrei
    endTime = new Date();
    System.out.println("Job ended: " + endTime);
    System.out.println("The Job1 took "
            + (endTime.getTime() - startTime.getTime()) / (float) 1000.0
            + " seconds.");
    //job2
    startTime = new Date();
    input = inputPathString;
    if (Configure.RunOnCluster) {
      output = "fs_prefix";
    } else {
      output = "/tmp/fs_prefix";
    }
    
    GeneratePrefixFileJob(input, output, similarityThreshold); // Liefert: RID, Originallänge, Präfix (wobei dieser sehr kurz ist: Beispiel RID 10044314: 5 von 20 Tokens! - unklar)
    endTime = new Date();
    System.out.println("The Job2 took " + (endTime.getTime() - startTime.getTime()) / (float) 1000.0 + " seconds");

    //Job 3
    startTime = new Date();
    input = inputPathString;
    output = "fs_ridpairs";
    IndexAndPartitionJob(input, output, similarityThreshold);
    endTime = new Date();
    System.out.println("The Job3 took "
            + (endTime.getTime() - startTime.getTime()) / (float) 1000.0
            + " seconds.");

    //Job4
    startTime = new Date();
    input = "fs_ridpairs";
    output = outputPathString;
    getRIDPairsJob(input, output, similarityThreshold);
    endTime = new Date();
    System.out.println("The Job4 took " + (endTime.getTime() - startTime.getTime()) / (float) 1000.0 + " seconds");
  }
}
