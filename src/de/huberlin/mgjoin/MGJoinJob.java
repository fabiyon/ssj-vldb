package de.huberlin.mgjoin;

import de.huberlin.textualsimilarityhadoop.ParameterParser;
import de.huberlin.vernicajoin.IntPairComparatorFirst;
import de.huberlin.vernicajoin.IntPairPartitionerFirst;
import de.huberlin.vernicajoin.IntPairWritable;
import edu.uci.ics.fuzzyjoin.similarity.SimilarityFiltersJaccard;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.TreeMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
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
public class MGJoinJob extends Configured implements Tool {
  
  @Override
  public int run(String[] args) throws Exception {
    boolean onlyStatistics = false;
    ParameterParser pp = new ParameterParser(args);
    
    Configuration conf = this.getConf();
    conf.set("delta", pp.getTheta()); // min. textual similarity
    FileSystem filesystem = FileSystem.get(getConf());

    String balancedString = pp.getOutput() + MGJoinDriver.BALANCED_PATH_SUFFIX;
    Path balancedPath = new Path(balancedString);

    String duplicateString = pp.getOutput() + MGJoinDriver.RESULTWITHDUPLICATES_PATH_SUFFIX;
    Path duplicatePath = new Path(duplicateString);
    filesystem.delete(duplicatePath, true);
    
    String globalTokenOrderPathString = pp.getOutput() + MGJoinDriver.TOKENSWITHFREQUENCIES_PATH_SUFFIX;
    conf.set("tokenFrequencyPath", globalTokenOrderPathString);

    System.out.println("MG Step 3: Replicate and Join");
    Job job3 = Job.getInstance(conf, "Step 3: Replicate and Join");
    job3.setJarByClass(MGJoinJob.class);
    
    job3.setMapperClass(MGReplicateMapper.class);
    job3.setMapOutputKeyClass(IntPairWritable.class);
    job3.setMapOutputValueClass(MGCanonicElem.class);
    job3.setPartitionerClass(IntPairPartitionerFirst.class);
    job3.setGroupingComparatorClass(IntPairComparatorFirst.class);
    
    if (onlyStatistics) {
      job3.setReducerClass(MGVerificationStatisticsReducer.class);
      Path outputPath = new Path(pp.getOutput());
      filesystem.delete(outputPath, true);
      FileOutputFormat.setOutputPath(job3, outputPath);
    } else {
      FileOutputFormat.setOutputPath(job3, duplicatePath);
      job3.setReducerClass(MGVerificationReducer.class);
    }
    

    job3.setOutputKeyClass(NullWritable.class);
    job3.setOutputValueClass(Text.class);

    FileInputFormat.addInputPath(job3, balancedPath);
    
    job3.waitForCompletion(true);
    
    return 0;
  }
  
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new Configuration(), new MGJoinJob(), args);
  }
  
  public static class MGReplicateMapper extends Mapper<Object, Text, IntPairWritable, MGCanonicElem> {
    private final IntPairWritable outKey = new IntPairWritable();
    private double delta;
    private SimilarityFiltersJaccard filter;
    private final TreeMap<Integer, Integer> tokenFreq = new TreeMap();
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      delta = Double.parseDouble(context.getConfiguration().get("delta"));
      filter = new SimilarityFiltersJaccard((float)delta, 1);
      
      Path pt = new Path(context.getConfiguration().get("tokenFrequencyPath"));// + "/part-r-00000"); /// <<<<<<<<<<< FALSCH! Es gibt mehr als eine Datei!
      FileSystem fs = FileSystem.get(new Configuration());
      RemoteIterator<LocatedFileStatus> it = fs.listFiles(pt, true);
      
      TreeMap<Integer, ArrayList<Integer>> intermediateTree = new TreeMap();
      
      while (it.hasNext()) {
        LocatedFileStatus lfs = it.next();
        if (lfs.isFile()) {
          BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(lfs.getPath())));
          String line = br.readLine();

          // we need an intermediate structure to sort the tokens according to their frequency:
          while (line != null) {
            String[] tmp = line.split("\\s+"); // tokenID, frequency. We don't need the frequency, so we only take the first array entry in the next line:
            Integer count = Integer.parseInt(tmp[1]);
            ArrayList<Integer> currentArrayList = intermediateTree.get(count);
            if (currentArrayList == null) {
              currentArrayList = new ArrayList();
              intermediateTree.put(count, currentArrayList);
            }
            currentArrayList.add(Integer.parseInt(tmp[0]));
            line = br.readLine();
          }
        }
      }
      
      int pos = 0;
      for(Map.Entry<Integer,ArrayList<Integer>> entry : intermediateTree.entrySet()) {
        ArrayList<Integer> valueList = entry.getValue();
        for (Integer val : valueList) {
          tokenFreq.put(val, pos++);
        }
      }
      
    }

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      MGCanonicElem outVal = new MGCanonicElem(value.toString());
      int prefixLength = filter.getPrefixLength(outVal.tokens.length);
      outVal.setPrefixLength(prefixLength);
      outVal.sortByGlobalTokenOrder(tokenFreq);
      outKey.setSecond(outVal.tokens.length);
     
      for (int i = 0; i < prefixLength; i++) { // we use the original token order as "random" order to replicate:
        outKey.setFirst(outVal.tokens[i]);
        context.write(outKey, outVal); // the value only contains the tokens sorted by global token order, no further prefix (optimization of Section 5.1 in the paper)
      }
      
    }
  }
  
  public static class MGVerificationStatisticsReducer extends Reducer<IntPairWritable, MGCanonicElem, NullWritable, Text> {
    private final NullWritable outKey = NullWritable.get();
    private final Text outVal = new Text();

    @Override
    public void reduce(IntPairWritable key, Iterable<MGCanonicElem> values, Context context) throws IOException, InterruptedException {
      int count = 0;
      Iterator<MGCanonicElem> it = values.iterator();
      while (it.hasNext()) {
        it.next();
        count++;
      }
      outVal.set(context.getTaskAttemptID().getTaskID() + " " + count);
      context.write(outKey, outVal);
    }
  }
  
  public static class MGVerificationReducer extends Reducer<IntPairWritable, MGCanonicElem, NullWritable, Text> {
    private final NullWritable outKey = NullWritable.get();
    private final Text outValue = new Text();
    private double delta;
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      delta = Double.parseDouble(context.getConfiguration().get("delta"));
    }

    @Override
    public void reduce(IntPairWritable key, Iterable<MGCanonicElem> values, Context context) throws IOException, InterruptedException {
      Iterator<MGCanonicElem> it = values.iterator();
      LinkedList<MGCanonicElem> prevEltsNew = new LinkedList(); // LinkedList hat keinen großen Vorteil gegenüber der ArrayList gebracht
      int minLength;
      Iterator<MGCanonicElem> prevEltsIt;
      MGCanonicElem prevElt;
      MGCanonicElem current;
      
      while (it.hasNext()) {
        current = it.next();
        minLength = (int)Math.ceil(delta * (double)current.tokensOrdered.length);
        prevEltsIt = prevEltsNew.iterator();
        
        while (prevEltsIt.hasNext()) {
          prevElt = prevEltsIt.next();
          if (prevElt.tokensOrdered.length < minLength) {
            prevEltsIt.remove();
          } else {
            // check if the intersection of the prefixes is > 0:
            if (prevElt.hasPrefixIntersectWith(current)) {
              if (prevElt.getJaccardSimilarityBetween(current) > delta) {
                outValue.set(current.getKey() + " " + prevElt.getKey());
                context.write(outKey, outValue);
              }
            }
          }
        }
        
        prevEltsNew.add(new MGCanonicElem(current));
      }
    }

  }
}
