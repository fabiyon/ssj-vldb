package de.huberlin.vernicajoin;

import de.huberlin.textualsimilarityhadoop.ParameterParser;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
public class DedupJob extends Configured implements Tool {
  
  @Override
  public int run(String[] args) throws Exception {
    ParameterParser pp = new ParameterParser(args);
    
    Configuration conf = this.getConf();
    FileSystem filesystem = FileSystem.get(getConf());

    Path ridPairPath = new Path(pp.getOutput() + VernicaJoinDriver.RID_PAIR_PATH);
    
    Path outputPath = new Path(pp.getOutput());
    filesystem.delete(outputPath, true);
    
    System.out.println("De-Duplication (OPRJ 'light')");
    Job job3 = Job.getInstance(conf, "De-Duplication");
    job3.setJarByClass(DedupJob.class);
    
    job3.setMapperClass(DedupMapper.class);
    job3.setMapOutputKeyClass(LongWritable.class);
    job3.setMapOutputValueClass(NullWritable.class);
    
    job3.setCombinerClass(DedupCombiner.class);
    
    job3.setReducerClass(DedupReducer.class);
    job3.setOutputKeyClass(Text.class);
    job3.setOutputValueClass(NullWritable.class);
    
    FileInputFormat.addInputPath(job3, ridPairPath);
    FileOutputFormat.setOutputPath(job3, outputPath);
    job3.waitForCompletion(true);
    
    return 0;
  }
  
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new Configuration(), new DedupJob(), args);
  }
  
  public static class DedupMapper extends Mapper<Object, Text, LongWritable, NullWritable> {
    private final LongWritable outKey = new LongWritable();
    private final NullWritable outValue = NullWritable.get();
    byte[] outBytes = new byte[8];

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String[] valArr = value.toString().split("\\s+");
      int val0 = Integer.parseInt(valArr[0]);
      int val1 = Integer.parseInt(valArr[1]);
      
      if (val0 == val1) {
        return;
      }
      
      if (val0 > val1) {
        int tmp = val0;
        val0 = val1;
        val1 = tmp;
      }
      
      // nur zum Testen des RxS-Joins
//      int split = 258715;
//      if ((val0 <= split && val1 <= split) || (val0 > split && val1 > split)) {
//        return;
//      }
      
      outKey.set(((long)val0 << 32) | ((long)val1 & 0xFFFFFFFL));
      context.write(outKey, outValue);
    }
  }
  
  public static class DedupCombiner extends Reducer<LongWritable, NullWritable, LongWritable, NullWritable> {
    private final NullWritable outValue = NullWritable.get();

    @Override
    public void reduce(LongWritable key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
      context.write(key, outValue);
    }
  }
  
  public static class DedupReducer extends Reducer<LongWritable, NullWritable, Text, NullWritable> {
    private final Text outKey = new Text();
    private final NullWritable outValue = NullWritable.get();

    @Override
    public void reduce(LongWritable key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
      long keyLong = key.get();
      int key0 = (int)(keyLong >> 32);
      int key1 = (int)keyLong;
      outKey.set(key0 + " " + key1);
      context.write(outKey, outValue);
    }
  }
}
