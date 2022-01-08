package de.huberlin.textualsimilarityhadoop;

import java.io.IOException;
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
public class TokenizeJob extends Configured implements Tool {
  
  @Override
  public int run(String[] args) throws Exception {
    System.out.println("Tokenize");
    ParameterParser pp = new ParameterParser(args);
    
    Configuration conf = this.getConf();
    String inputPath = pp.getInput();
    String outputPath = pp.getOutput();
    
    Job job1a = Job.getInstance(conf, "List all tokens as strings duplicate-free");
    job1a.setJarByClass(TokenizeJob.class);
    
    job1a.setMapperClass(TokenizeMapper.class);
    job1a.setMapOutputKeyClass(Text.class);
    job1a.setMapOutputValueClass(NullWritable.class);
    
    job1a.setReducerClass(TokenizeReducer.class);
    job1a.setCombinerClass(TokenizeReducer.class);
    job1a.setOutputKeyClass(Text.class);
    job1a.setOutputValueClass(NullWritable.class);
    
    FileInputFormat.addInputPath(job1a, new Path(inputPath));
    SequenceFileAsBinaryOutputFormat.setOutputPath(job1a, new Path(outputPath));
    
    job1a.waitForCompletion(true);
    return 0; 
  }
  
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new TokenizeJob(), args);
    System.exit(res);
  }
  
  public static class TokenizeMapper extends Mapper<Object, Text, Text, NullWritable> {
    private final Text outKey = new Text();
    private final NullWritable outValue = NullWritable.get();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String[] valArr = value.toString().split("\\s+");
      for (String valArr1 : valArr) {
        outKey.set(valArr1);
        context.write(outKey, outValue);
      }
    }
  }
  
  public static class TokenizeReducer extends Reducer<Text, IntWritable, Text, NullWritable> {
    private final Text outKey = new Text();
    private final NullWritable outValue = NullWritable.get();

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      outKey.set(key);
      context.write(outKey, outValue);
    }
  }
}
