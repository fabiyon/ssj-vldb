package de.huberlin.massjoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import de.huberlin.textualsimilarityhadoop.ParameterParser;
import de.huberlin.vernicajoin.IntPairComparatorFirst;
import de.huberlin.vernicajoin.IntPairPartitionerFirst;
import de.huberlin.vernicajoin.IntPairWritable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;


/**
 *
 * @author fabi
 */
public class Step3RJoinJob extends Configured implements Tool {

  @Override
  public int run(String[] args) throws Exception {
    ParameterParser pp = new ParameterParser(args);
    
    Configuration conf = this.getConf();
    conf.set("delta", pp.getTheta());
//    conf.set("mapreduce.map.output.compress", "true");
    
    Path inputPath = new Path(pp.getInput());
    Path intermediateVerificationPath = new Path(pp.getOutput() + MassJoinDriver.SJOINOUT_PATH);
    Path outputPath = new Path (pp.getOutput());
    FileSystem filesystem = FileSystem.get(getConf());
    filesystem.delete(outputPath, true);

    // --------------- Step 3: Verification 2 ---------------------
    System.out.println("MassJoin: Verification 2");
    Job job3 = Job.getInstance(conf, "Verification 2");
    job3.setJarByClass(Step3RJoinJob.class);
//    job.setCombinerClass(GroupCreateReducer.class);
    
    job3.setMapOutputKeyClass(IntPairWritable.class);
    job3.setMapOutputValueClass(Text.class);
    
    MultipleInputs.addInputPath(job3, inputPath, MassJoinS3InputFormat.class, Mapper.class); // Mapper is the new IdentityMapper AND this addInputPath method requires the third argument!
    MultipleInputs.addInputPath(job3, intermediateVerificationPath, TextInputFormat.class, RreaderMapper.class);
    
    job3.setPartitionerClass(IntPairPartitionerFirst.class);
    job3.setGroupingComparatorClass(IntPairComparatorFirst.class);
    
    if (job3.getNumReduceTasks() < 2) {
      job3.setNumReduceTasks(2); // this needs to be set in local environment to trigger the non-default partitioner etc. 
    }
    
    job3.setReducerClass(RjoinReducer.class);
    job3.setOutputKeyClass(LongWritable.class);
    job3.setOutputValueClass(Text.class);
    
    FileOutputFormat.setOutputPath(job3, outputPath);
    return job3.waitForCompletion(true) ? 0 : 1;
  }
  
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new Configuration(), new Step3RJoinJob(), args);
  }
  
  public static class RreaderMapper extends Mapper<Object, Text, IntPairWritable, Text> {
    private final IntPairWritable outKey = new IntPairWritable();
    private final Text outValue = new Text();
    
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String valString = value.toString();
      String[] valArr = valString.split("\\s+");
      String[] ridArr = valArr[valArr.length - 1].split("_");
      outValue.set(valArr[0] + " " + valArr[1]); // remove the "_"-divided tokens from the string to save network
      for (String rid : ridArr) {
        outKey.set(Integer.parseInt(rid), 1);
        context.write(outKey, outValue);
      }
    }
  }
  
  public static class RjoinReducer extends Reducer<IntPairWritable, Text, Text, NullWritable> {
    private final Text outKey = new Text();
    private final NullWritable outVal = NullWritable.get();
    private double delta;
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      delta = Double.parseDouble(context.getConfiguration().get("delta"));
    }

    @Override
    public void reduce(IntPairWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      Iterator<Text> it = values.iterator();
      int[] r;
      int rId = key.getFirst();
      
      // the first record is from r:
      Text currentText = it.next();
      String[] valArr = currentText.toString().split("\\s+");
      if (valArr.length > 1 && key.getSecond() == 0) { // otherwise: bad data
        r = stringToTokenArr(valArr[1]);
      } else {
        return;
      }
      
      int[] s;
      double similarity;
      while (it.hasNext()) {
        currentText = it.next();
        valArr = currentText.toString().split("\\s+");
        s = stringToTokenArr(valArr[1]);
        similarity = jaccardSimilarity(r, s);
        if (similarity > delta) {
          outKey.set(rId + " " + valArr[0]);
          context.write(outKey, outVal);
        }
      }
    }
    
    private int[] stringToTokenArr(String input) {
      String[] tokenArr = input.split(",");
      int[] r = new int[tokenArr.length];
      for (int i = 0; i < tokenArr.length; i++) {
        r[i] = Integer.parseInt(tokenArr[i]);
      }
      return r;
    } 
    
    // We implement a nested loop join here, because we do not have the tokens
    // ordered by their numeric values anymore:
    public static double jaccardSimilarity(int[] set1, int[] set2) {
      double intersection = 0;
      for (int i = 0; i < set1.length; i++) {
        for (int j = 0; j < set2.length; j++) {
          if (set2[j] == set1[i]) {
            intersection++;
          }
        }
      }
      return (double) (intersection / (set1.length + set2.length - intersection));
    }
  }
}
