package de.huberlin.textualsimilarityhadoop;

import de.huberlin.vernicajoin.AsymFuzzyJoinMemory;
import de.huberlin.vernicajoin.ResultJoin;
import de.huberlin.vernicajoin.ResultSelfJoin;
import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.logging.Level;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

public class AsymJoinJob extends Configured implements Tool {
  private static final Logger log = Logger.getLogger(AsymJoinJob.class);

  public static class AsymJoinMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
    public double delta;
    public boolean joinRaw = false;
    private final NullWritable k = NullWritable.get();
    private final Text val = new Text();
    private String input;
    
    private void join(String fileName, HashMap<Integer, Integer> rids, Context context, AsymFuzzyJoinMemory j, boolean doSelfJoin) throws FileNotFoundException, IOException, InterruptedException {
      Path pt = new Path(fileName);
      FileSystem fs = FileSystem.get(context.getConfiguration());
      BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
      try {
        String line;
        int crtRecordId = 0;
        while ((line = br.readLine()) != null){
          String[] tuple = line.split("\\s+");
          if (tuple.length < 2) { // disregard empty records
            continue;
          }
          String[] tokenStr = tuple[1].split(",");
          int[] record = new int[tokenStr.length];
          for (int i = 0; i < tokenStr.length; i++) {
            record[i] = Integer.parseInt(tokenStr[i]);
          }
          if (doSelfJoin) {
            rids.put(crtRecordId++, Integer.parseInt(tuple[0])); // fill the rid translate list
            ArrayList<ResultSelfJoin> results = j.selfJoinAndAddRecord(record);
            for (ResultSelfJoin result : results) {
              val.set(rids.get(result.indexX) + " " + rids.get(result.indexY)); // translate the "local" rids of the result back to the original ones
              context.write(k, val);
            }
          } else { // RxS join with the indexed records:
            ArrayList<ResultJoin> results = j.join(record, record.length);
            for (ResultJoin result : results) {
              val.set(tuple[0] + " " + rids.get(result.index)); // translate the "local" rids of the result back to the original ones
              context.write(k, val);
            }
          }
        }
      } finally {
        // you should close out the BufferedReader
        br.close();
      }
    }
   
    @Override
    public void map(LongWritable inputKey, Text inputValue, Context context) throws IOException, InterruptedException {
      AsymFuzzyJoinMemory j = new AsymFuzzyJoinMemory((float)delta);
      HashMap<Integer, Integer> rids = new HashMap(); // translates "local" rids from the join (0..N) to the actual rids from the input
      boolean foundIndexPartition = false;
      FileSystem fs = FileSystem.get(new Configuration());
      FileStatus[] fileStatus = fs.listStatus(new Path(input));
      for (FileStatus status : fileStatus) {
        // status.getPath().toString() returns i.e., file:/home/fabi/researchProjects/textualSimilarity/data/input/4
        // status.getPath().getName() returns the filename
        if (status.getPath().getName().equals(inputValue.toString())) { // index partition assigned to this mapper. Self-Join it on the way
          this.join(status.getPath().toString(), rids, context, j, true);
          foundIndexPartition = true;
        }
      }
      if (!foundIndexPartition) { // There may be more Map instances than we need:
        return;
      }
      for (FileStatus status : fileStatus) { // we iterate throuhg the list a second time, because the order of the files is not necessarily lexicographically, but we need the index partition to be processed first:
        if (Integer.parseInt(status.getPath().getName()) < Integer.parseInt(inputValue.toString())) {
          this.join(status.getPath().toString(), rids, context, j, false);
        }
      }
    }
    
    @Override
    public void setup(Context context) {
      delta = context.getConfiguration().getDouble("delta", 0);
      input = context.getConfiguration().get("input", "");
    }
  }
  
  @Override
  public int run(String[] args) throws Exception {
    System.out.println("AsymJoin");
//    log.setLevel(Level.ALL);
    ParameterParser pp = new ParameterParser(args);
    
    Configuration conf = this.getConf();
    conf.set("delta", pp.getTheta());
    conf.set("mapreduce.input.fileinputformat.split.maxsize", "2"); // File with one integer per line causes one Map instance per line
    conf.set("input", pp.getInput());
    
    String outputPath = pp.getOutput();
    FileSystem filesystem = FileSystem.get(getConf());
    filesystem.delete(new Path(outputPath), true);
    
    System.out.println("Join Step");
    Job job1a = Job.getInstance(conf, "Join Step");
    job1a.setJarByClass(AsymJoinJob.class);
    
    job1a.setMapperClass(AsymJoinMapper.class);
    job1a.setMapOutputKeyClass(NullWritable.class);
    job1a.setMapOutputValueClass(Text.class);
    
//    FileInputFormat.addInputPath(job1a, new Path("file:///home/fabi/researchProjects/textualSimilarity/data/test")); // file with filenames hdfs:///user/fier/
    FileInputFormat.addInputPath(job1a, new Path("hdfs:///user/fier/distribasym1")); // file with filenames 
    FileOutputFormat.setOutputPath(job1a, new Path(outputPath));
    
    return job1a.waitForCompletion(true) ? 0 : 1;
  }
  
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new Configuration(), new AsymJoinJob(), args);
  }
  
}