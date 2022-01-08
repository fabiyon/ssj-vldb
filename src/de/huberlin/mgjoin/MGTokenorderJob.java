package de.huberlin.mgjoin;

import de.huberlin.textualsimilarityhadoop.ParameterParser;
import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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
public class MGTokenorderJob extends Configured implements Tool {
  
  @Override
  public int run(String[] args) throws Exception {
    ParameterParser pp = new ParameterParser(args);
    
    Configuration conf = this.getConf();
    FileSystem filesystem = FileSystem.get(getConf());
    Path inputPath = new Path(pp.getInput());
    
    Path tokensWithFrequenciesPath = new Path(pp.getOutput() + MGJoinDriver.TOKENSWITHFREQUENCIES_PATH_SUFFIX);
    filesystem.delete(tokensWithFrequenciesPath, true);

    // Compute global token order(s):
    Job job1a = Job.getInstance(conf, "MG Step 1a: Count term frequencies");
    job1a.setJarByClass(MGTokenorderJob.class);
    
    job1a.setMapperClass(MGSplitMapper.class);
    job1a.setMapOutputKeyClass(IntWritable.class);
    job1a.setMapOutputValueClass(IntWritable.class);
    
    job1a.setReducerClass(MGCountReducer.class);
    job1a.setCombinerClass(MGCountCombiner.class);
    job1a.setOutputKeyClass(IntWritable.class); // TokenID
    job1a.setOutputValueClass(IntWritable.class); // Global Frequency
    if (job1a.getNumReduceTasks() < 2) {
      job1a.setNumReduceTasks(2); // to test locally if this works also with more than 1 reducer
    }
    
    FileInputFormat.addInputPath(job1a, inputPath);
    FileOutputFormat.setOutputPath(job1a, tokensWithFrequenciesPath);
    
    job1a.waitForCompletion(true);
    
    return 0;
  }
  
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new MGTokenorderJob(), args);
//    System.exit(res);
  }
  
  
  public static class MGSplitMapper extends Mapper<Object, Text, IntWritable, IntWritable> {
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
  
  public static class MGCountCombiner extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
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
  
  public static class MGCountReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
    private final IntWritable outKey = new IntWritable();
    private final IntWritable outVal = new IntWritable();

    @Override
    public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      Iterator<IntWritable> it = values.iterator();

      int sum = 0;
      while (it.hasNext()) {
        IntWritable currentText = it.next();
        sum += currentText.get();
      }

      outKey.set(key.get());
      outVal.set(sum);

      context.write(outKey, outVal);

    }
  }
  
}
