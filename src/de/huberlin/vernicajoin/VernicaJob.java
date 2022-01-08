package de.huberlin.vernicajoin;

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

/**
 * Input: 1. RID TokenListCommaSeparated 2. Setup: TokenID Rank Output: RID RID
 */
public class VernicaJob extends Configured implements Tool {

  @Override
  public int run(String[] args) throws Exception {
    boolean onlyStatistics = false;
    ParameterParser pp = new ParameterParser(args);

    Configuration conf = this.getConf();
    conf.set("delta", pp.getTheta());
    FileSystem filesystem = FileSystem.get(getConf());

    String inputPathString = pp.getInput();
    Path inputPath = new Path(inputPathString);

    String ridPairPath = pp.getOutput() + VernicaJoinDriver.RID_PAIR_PATH;

    Path ridPairPath0 = new Path(ridPairPath);
    filesystem.delete(ridPairPath0, true);

    String tokenFrequencyPathString = pp.getOutput() + VernicaJoinDriver.TOKENORDER_PATH_SUFFIX;
    conf.set("tokenFrequencyPath", tokenFrequencyPathString);

    System.out.println("Vernica PK: PPJoin+-Based Signature Creation and Join");
    Job job2 = Job.getInstance(conf, "PPJoin+-Based Signature Creation and Join");
    job2.setJarByClass(VernicaJob.class);

    job2.setMapperClass(SignatureCreationMapper.class);
    job2.setMapOutputKeyClass(IntPairWritable.class); // tokenId, length. Length is needed for sorting by length and pruning the index
    job2.setMapOutputValueClass(VernicaElem.class);
    job2.setPartitionerClass(IntPairPartitionerFirst.class);
    job2.setGroupingComparatorClass(IntPairComparatorFirst.class);

    if (onlyStatistics) {
      job2.setReducerClass(JoinStatisticsReducer.class);
      Path outputPath = new Path(pp.getOutput());
      filesystem.delete(outputPath, true);
      FileOutputFormat.setOutputPath(job2, outputPath);
    } else {
      FileOutputFormat.setOutputPath(job2, ridPairPath0);
      job2.setReducerClass(JoinReducer.class);
    }
    
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(NullWritable.class);

    FileInputFormat.addInputPath(job2, inputPath);
    
    job2.waitForCompletion(true);

    return 0;
  }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new Configuration(), new VernicaJob(), args);
  }

  public static class SignatureCreationMapper extends Mapper<Object, Text, IntPairWritable, VernicaElem> {

    private final IntPairWritable outKey = new IntPairWritable();
//    private final VernicaElem outVal = new VernicaElem();
    private final TreeMap<Integer, Integer> tokenFreq = new TreeMap();
    private double delta;
    private SimilarityFiltersJaccard filter;

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
      delta = Double.parseDouble(context.getConfiguration().get("delta"));
      filter = new SimilarityFiltersJaccard((float) delta, 1);
//      log.setLevel(Level.ALL);
    }

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      VernicaElem outVal = new VernicaElem(); // maybe reuse leads to wrong results. No, maybe not.  <<<<<<<<<<<<<<<<<
      String[] tuple = value.toString().split("\\s+");
      if (tuple.length < 2) {
        return;
      }
      int keyInt = Integer.parseInt(tuple[0]);

      outVal.setKey(keyInt);
      String[] tokens = tuple[1].split(",");

      TreeMap<Integer, Integer> sortedTokens = new TreeMap();
      int prefixLength = filter.getPrefixLength(tokens.length); // we need to take the probe prefix here:
      // since we join based on equality, the prefix tokens are index and probe prefix at the same time. 
      // If we take the index prefix, we loose results.

      for (String token : tokens) {
        int tokenId = Integer.parseInt(token);
        sortedTokens.put(tokenFreq.get(tokenId), tokenId);
      }

      outVal.setTokens(sortedTokens);

      int position = 0;
      int tokenId;
      for (Map.Entry<Integer, Integer> entry : sortedTokens.entrySet()) {
        tokenId = entry.getValue();
        if (position <= prefixLength) {
          int outKeyInt = tokenFreq.get(tokenId);
          outKey.set(outKeyInt, tokens.length); // we simply replace the tokens with the position in the frequencies to make the PPJoin work
          context.write(outKey, outVal);
        } else {
          break;
        }
        position++;
      }
    }
  }

  public static class JoinReducer extends Reducer<IntPairWritable, VernicaElem, Text, NullWritable> {

    private final Text outKey = new Text();
    private final NullWritable outputValue = NullWritable.get();
    private float delta;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      delta = Float.parseFloat(context.getConfiguration().get("delta"));
    }

    @Override
    public void reduce(IntPairWritable key, Iterable<VernicaElem> values, Context context) throws IOException, InterruptedException {
      HashMap<Integer, Integer> rids = new HashMap();
      FuzzyJoinMemory fuzzyJoinMemory = new FuzzyJoinMemory(delta);
      int crtRecordId = 0;
      Iterator<VernicaElem> it = values.iterator();

      while (it.hasNext()) {
        VernicaElem current = it.next();
        int[] record = current.getTokens();
        rids.put(crtRecordId, (int) current.getKey());
        crtRecordId++;
        ArrayList<ResultSelfJoin> results = fuzzyJoinMemory
                .selfJoinAndAddRecord(record);
        for (ResultSelfJoin result : results) {
          int rid1 = rids.get(result.indexX);
          int rid2 = rids.get(result.indexY);
          if (rid1 < rid2) {
            int rid = rid1;
            rid1 = rid2;
            rid2 = rid;
          }

          outKey.set(rid1 + " " + rid2);
          context.write(outKey, outputValue);
        }

      }
    }

  }

  public static class JoinStatisticsReducer extends Reducer<IntPairWritable, VernicaElem, Text, NullWritable> {

    private final Text outKey = new Text();
    private final NullWritable outVal = NullWritable.get();

    @Override
    public void reduce(IntPairWritable key, Iterable<VernicaElem> values, Context context) throws IOException, InterruptedException {
      int count = 0;
      Iterator<VernicaElem> it = values.iterator();
      while (it.hasNext()) {
        it.next();
        count++;
      }
      outKey.set(context.getTaskAttemptID().getTaskID() + " " + count);
      context.write(outKey, outVal);
    }

  }
}
