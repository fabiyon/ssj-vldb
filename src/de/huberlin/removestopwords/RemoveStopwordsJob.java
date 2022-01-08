package de.huberlin.removestopwords;

import de.huberlin.vernicajoin.*;
import de.huberlin.textualsimilarityhadoop.ParameterParser;
import edu.uci.ics.fuzzyjoin.similarity.SimilarityFiltersJaccard;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class RemoveStopwordsJob extends Configured implements Tool {

  @Override
  public int run(String[] args) throws Exception {
    ParameterParser pp = new ParameterParser(args);

    Configuration conf = this.getConf();
    FileSystem filesystem = FileSystem.get(getConf());

    String inputPathString = pp.getInput();
    Path inputPath = new Path(inputPathString);
    
    String outputString = pp.getOutput();
    Path outputPath = new Path(outputString);
    filesystem.delete(outputPath, true);

    String tokenFrequencyPathString = pp.getOutput() + VernicaJoinDriver.TOKENORDER_PATH_SUFFIX;
    conf.set("tokenFrequencyPath", tokenFrequencyPathString);

    System.out.println("Remove Stopwords");
    Job job2 = Job.getInstance(conf, "Remove Stopwords");
    job2.setJarByClass(RemoveStopwordsJob.class);

    job2.setMapperClass(RemoveStopwordsMapper.class);
    job2.setMapOutputKeyClass(NullWritable.class); // tokenId, length. Length is needed for sorting by length and pruning the index
    job2.setMapOutputValueClass(Text.class);
    
    FileInputFormat.addInputPath(job2, inputPath);
    FileOutputFormat.setOutputPath(job2, outputPath);
    job2.waitForCompletion(true);

    return 0;
  }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new Configuration(), new RemoveStopwordsJob(), args);
  }

  public static class RemoveStopwordsMapper extends Mapper<Object, Text, NullWritable, Text> {

    private final NullWritable outKey = NullWritable.get();
    private final Text outVal = new Text();
    private final TreeMap<Integer, Integer> tokenFreq = new TreeMap();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      try {
        Path pt = new Path(context.getConfiguration().get("tokenFrequencyPath") + "/part-r-00000");
        FileSystem fs = FileSystem.get(new Configuration());
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
        String line = br.readLine();
        int pos = 0;
        while (line != null) {
          String[] tmp = line.split("\\s+"); // tokenID, frequency. We don't need the frequency, so we only take the first array entry in the next line:
          tokenFreq.put(Integer.parseInt(tmp[0]), pos++); // Achtung: die Tokens sind nicht aufsteigend nach Integer sortiert, sondern nach globaler Häufigkeit
          line = br.readLine();
        }
      } catch (Exception e) {
      }
    }

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String[] tuple = value.toString().split("\\s+");
      if (tuple.length < 2) {
        return;
      }
      String outString = ""; // rid

      // ignoriere die häufigsten 1%:
      int onePercent = (int)((double)tokenFreq.size() * 0.01);
      int ignoreIdFrom = tokenFreq.size() - onePercent;
      
      String[] tokens = tuple[1].split(",");
      for (String token : tokens) {
        int position = tokenFreq.get(Integer.parseInt(token));
        if (position < ignoreIdFrom) {
          if (!outString.isEmpty()) {
            outString += ",";
          }
          outString += token;
        }
      }
      if (!outString.isEmpty()) {
        outString = tuple[0] + " " + outString;
        outVal.set(outString);
        context.write(outKey, outVal);
      }
    }
  }

}
