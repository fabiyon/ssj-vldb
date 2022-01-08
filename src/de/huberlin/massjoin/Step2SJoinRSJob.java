package de.huberlin.massjoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import de.huberlin.textualsimilarityhadoop.ParameterParser;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author fabi
 */
public class Step2SJoinRSJob extends Configured implements Tool {

  @Override
  public int run(String[] args) throws Exception {
    boolean onlyStatistics = true;
    ParameterParser pp = new ParameterParser(args);

    Configuration conf = this.getConf();
//    conf.set("mapreduce.map.output.compress", "true");

    Path inputPath = new Path(pp.getInputS());
    Path intermediateCandidatePath = new Path(pp.getOutput() + MassJoinDriver.CANDIDATE_PATH);
    Path sJoinOutPath = new Path(pp.getOutput() + MassJoinDriver.SJOINOUT_PATH);
    FileSystem filesystem = FileSystem.get(getConf());
    filesystem.delete(sJoinOutPath, true);

    // --------------- Step 2: Verification 1 ---------------------
    System.out.println("MassJoin: Verification 1");
    Job job2 = Job.getInstance(conf, "Verification 1");
    job2.setJarByClass(Step2SJoinRSJob.class);

    job2.setMapOutputKeyClass(IntWritable.class);
    job2.setMapOutputValueClass(Text.class);
    job2.setInputFormatClass(MassJoinS2InputFormat.class);
    
    
    job2.setReducerClass(SjoinReducer.class);
    FileOutputFormat.setOutputPath(job2, sJoinOutPath);
    
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(Text.class);

    FileInputFormat.addInputPath(job2, intermediateCandidatePath);
    FileInputFormat.addInputPath(job2, inputPath);
    

    return job2.waitForCompletion(true) ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new Configuration(), new Step2SJoinRSJob(), args);
  }

  
  public static class SjoinReducer extends Reducer<IntWritable, Text, Text, NullWritable> {

    private final Text outKey = new Text();
    private final NullWritable outValue = NullWritable.get();

    @Override
    public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      Iterator<Text> it = values.iterator();
      HashSet<Integer> candidates = new HashSet();
      String s = ""; // exists only once
      while (it.hasNext()) {
        Text currentText = it.next();
        String[] valArr = currentText.toString().split("\\s+");
        if (valArr.length == 3) { // this is a result from the previous MR step:
          String[] candArr = valArr[1].split("_");
          for (String cand : candArr) {
            candidates.add(Integer.parseInt(cand)); // should assure that each token is saved only once in the array
          }
        } else { // this is a raw input:
          s = currentText.toString();
        }
      }
      String candidatesString = "";
      for (Integer candidate : candidates) {
        if (!candidatesString.equals("")) {
          candidatesString += "_";
        }
        candidatesString += candidate;
      }
      if (!candidatesString.equals("")) {
        outKey.set(s + " " + candidatesString);
        context.write(outKey, outValue);
      }

    }
  }
}
