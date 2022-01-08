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
import de.huberlin.textualsimilarityhadoop.SSJ2RNew;
import de.huberlin.vernicajoin.VernicaJoinDriver;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

/**
 *
 * @author fabi
 */
public class Step1SignatureCreationRSJob extends Configured implements Tool {

  @Override
  public int run(String[] args) throws Exception {
    ParameterParser pp = new ParameterParser(args);
    
    Configuration conf = this.getConf();
    conf.set("delta", pp.getTheta());
    
    Path inputPathR = new Path(pp.getInput());
    Path inputPathS = new Path(pp.getInputS());
    Path outputCandidatePath = new Path(pp.getOutput() + MassJoinDriver.CANDIDATE_PATH);
    FileSystem filesystem = FileSystem.get(getConf());
    filesystem.delete(outputCandidatePath, true);
    conf.set("tokenFrequencyPath", pp.getOutput() + VernicaJoinDriver.TOKENORDER_PATH_SUFFIX);
    
    // ------------- Step 1: Signature creation -------------------
    System.out.println("MassJoin Step 1: Signature Creation");
    Job job1 = Job.getInstance(conf, "Step 1: Signature Creation");
    job1.setJarByClass(Step1SignatureCreationRSJob.class);
    
    job1.setMapOutputKeyClass(MassJoinSignatureKey.class);
    job1.setMapOutputValueClass(MassJoinIntermediateElem.class);
    if (job1.getNumReduceTasks() < 2) {
      job1.setNumReduceTasks(2); // this needs to be set in local environment to trigger the non-default partitioner etc. 
    }
    // partitioning is done via hash of MassJoinSignatureKey, which is over the token array
    job1.setSortComparatorClass(MassJoinSignatureSortComparator.class);
    job1.setGroupingComparatorClass(MassJoinSignatureGroupComparator.class);
    
    job1.setReducerClass(CandidateCreationReducer.class);
    job1.setOutputKeyClass(IntWritable.class);
    job1.setOutputValueClass(Text.class);
    
//    FileInputFormat.addInputPath(job1, inputPath);
    MultipleInputs.addInputPath(job1, inputPathR, TextInputFormat.class, SignatureMapperR.class);
    MultipleInputs.addInputPath(job1, inputPathS, TextInputFormat.class, SignatureMapperS.class);
    
    //<<<<<<<<<<<<<<<<<<<
//    SequenceFileAsBinaryOutputFormat.setOutputPath(job1, new Path(intermediateCandidatePath));
//    job1.setOutputFormatClass(SequenceFileAsBinaryOutputFormat.class);
    FileOutputFormat.setOutputPath(job1, outputCandidatePath);
    // <<<<<<<<<<<<<<<<<

    int returnCode = job1.waitForCompletion(true) ? 0 : 1;

    return returnCode;
  }
  
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new Configuration(), new Step1SignatureCreationRSJob(), args);
  }
  
  public static class SignatureMapperR extends Mapper<Object, Text, MassJoinSignatureKey, MassJoinIntermediateElem> {
    private final MassJoinSignatureKey outKey = new MassJoinSignatureKey();
    private final MassJoinIntermediateElem outValue = new MassJoinIntermediateElem();
    private double delta;
    private final TreeMap<Integer, Integer> tokenGroupAssignment = new TreeMap();
    private final int[] groupSums = new int[30]; // temporarily needed to get the next smallest group (greedy algorithm) of MassJoin
    
    private Integer getNextSmallestGroupAndAdd(int newSum) {
      int smallestSum = Integer.MAX_VALUE;
      int idOfSmallestSum = 0;
      for (int i = 0; i < groupSums.length; i++) {
        int currentSum = groupSums[i];
        if (currentSum < smallestSum) {
          smallestSum = currentSum;
          idOfSmallestSum = i;
        }
      }
      groupSums[idOfSmallestSum] += newSum;
      return idOfSmallestSum;
    }
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      delta = Double.parseDouble(context.getConfiguration().get("delta"));
      try {
        Path pt = new Path(context.getConfiguration().get("tokenFrequencyPath") + "/part-r-00000");
        FileSystem fs = FileSystem.get(new Configuration());
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
        String line = br.readLine();
        while (line != null) {
          String[] tmp = line.split("\\s+");
          tokenGroupAssignment.put(Integer.parseInt(tmp[0]), getNextSmallestGroupAndAdd(Integer.parseInt(tmp[1]))); // TokenID, GroupID
          // (Die Tokens sind nicht aufsteigend nach Integer sortiert, sondern nach globaler Häufigkeit. Das Häufigste Token steht ganz am Ende.)
          line = br.readLine();
        }
      } catch(Exception e) {
      }
      if (tokenGroupAssignment.size() == 0) {
        throw new IOException("No token group assignment! Maybe token file was unreadable or didn't contain anything.");
      }
    }
    
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String[] valueArr = value.toString().split("\\s+");
      
      if (valueArr.length < 2) {
        return;
      } 
      int rID = Integer.parseInt(valueArr[0]);
      String[] tokenArr = valueArr[1].split(",");
      int[] r = new int[tokenArr.length];
      int l = 0;
      for (String token : tokenArr) {
        r[l++] = Integer.parseInt(token);
      }
//      Arrays.sort(r); // our generated datasets require that...
      
      // Generate textual signatures:
      // Lightweight Filter Unit (grouping):
      int[] filterUnitR = new int[30];
      for (int index = 0; index < r.length; index++) {
        // 30 groups are the optimal value in the paper, see VII.A
        try {
          int groupId = tokenGroupAssignment.get(r[index]);
          filterUnitR[groupId] += 1; // richtig: jedes Token muss zwingend in der Häufigkeitsliste enthalten sein. Jede auftretende Gruppe wird hochgezählt: ok!
        } catch (NullPointerException e) {
          System.out.println(e);
        }
      }

      // ================== Signature creation for R =========================================
      int absR = r.length;
      int u_r_s = (int) Math.floor(Math.round(1000 * (absR - delta * Math.floor(absR / delta)) / (1 + delta)) / 1000); // =U_{r-s} - richtig, aber komisch mit den 1000ern
      int u_s_r = (int) Math.floor(Math.round(1000 * (Math.floor(absR / delta) - delta * absR) / (1 + delta) / 1000)); // =U_{s-r} - richtig, aber komisch mit den 1000ern
      int UQuer = u_r_s + u_s_r; // richtig
      int dreiEck = UQuer + 1; // richtig
      int k = (int) (absR - Math.floor(absR / (dreiEck)) * (dreiEck)); // richtig. Die letzten k Segmente sind die der größeren Länge.
      int kleinere = (int) Math.floor(absR / (UQuer + 1)); // Größe der kleineren Segmente am Anfang: richtig
      int groessere = kleinere + 1; // Größe der größeren Segmente an Ende: richtig
      int lil; // length of the current segment. l_{i}
      int startPosition = 0;
      for (int i = 0; i < dreiEck; i++) { // for each segment:
        // get the correct length of the current segment:
        if (i < dreiEck - k) { // richtig
          lil = kleinere;
        } else { // richtig
          lil = groessere;
        }
        
        if (lil != 0) {
          int[] signatur = Arrays.copyOfRange(r, startPosition, startPosition + lil);
          outKey.set(signatur, 0);
          outValue.set(0, i, absR, rID, filterUnitR);
//          System.out.println(outKey + " " + outValue);
          context.write(outKey, outValue);
        }
        startPosition += lil;
      }

    }
  }
  
  public static class SignatureMapperS extends Mapper<Object, Text, MassJoinSignatureKey, MassJoinIntermediateElem> {
    private final MassJoinSignatureKey outKey = new MassJoinSignatureKey();
    private final MassJoinIntermediateElem outValue = new MassJoinIntermediateElem();
    private double delta;
    private final TreeMap<Integer, Integer> tokenGroupAssignment = new TreeMap();
    private final int[] groupSums = new int[30]; // temporarily needed to get the next smallest group (greedy algorithm) of MassJoin
    
    private Integer getNextSmallestGroupAndAdd(int newSum) {
      int smallestSum = Integer.MAX_VALUE;
      int idOfSmallestSum = 0;
      for (int i = 0; i < groupSums.length; i++) {
        int currentSum = groupSums[i];
        if (currentSum < smallestSum) {
          smallestSum = currentSum;
          idOfSmallestSum = i;
        }
      }
      groupSums[idOfSmallestSum] += newSum;
      return idOfSmallestSum;
    }
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      delta = Double.parseDouble(context.getConfiguration().get("delta"));
      try {
        Path pt = new Path(context.getConfiguration().get("tokenFrequencyPath") + "/part-r-00000");
        FileSystem fs = FileSystem.get(new Configuration());
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
        String line = br.readLine();
        while (line != null) {
          String[] tmp = line.split("\\s+");
          tokenGroupAssignment.put(Integer.parseInt(tmp[0]), getNextSmallestGroupAndAdd(Integer.parseInt(tmp[1]))); // TokenID, GroupID
          // (Die Tokens sind nicht aufsteigend nach Integer sortiert, sondern nach globaler Häufigkeit. Das Häufigste Token steht ganz am Ende.)
          line = br.readLine();
        }
      } catch(Exception e) {
      }
      if (tokenGroupAssignment.size() == 0) {
        throw new IOException("No token group assignment! Maybe token file was unreadable or didn't contain anything.");
      }
    }
    
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String[] valueArr = value.toString().split("\\s+");
      
      if (valueArr.length < 2) {
        return;
      } 
      int rID = Integer.parseInt(valueArr[0]);
      String[] tokenArr = valueArr[1].split(",");
      int[] r = new int[tokenArr.length];
      int l = 0;
      for (String token : tokenArr) {
        r[l++] = Integer.parseInt(token);
      }
//      Arrays.sort(r); // our generated datasets require that...
      
      // Generate textual signatures:
      // Lightweight Filter Unit (grouping):
      int[] filterUnitR = new int[30];
      for (int index = 0; index < r.length; index++) {
        // 30 groups are the optimal value in the paper, see VII.A
        try {
          int groupId = tokenGroupAssignment.get(r[index]);
          filterUnitR[groupId] += 1; // richtig: jedes Token muss zwingend in der Häufigkeitsliste enthalten sein. Jede auftretende Gruppe wird hochgezählt: ok!
        } catch (NullPointerException e) {
          System.out.println(e);
        }
      }
      
      // ================== Signature creation for S =========================================
      double absS = r.length;
      double Lo = Math.ceil(delta * absS);
      double Lu = (Math.floor(absS / delta));
      int dreiEckLo = (int) Math.floor(Math.round(1000 * (Lo - delta * Math.floor(Lo / delta)) / (1 + delta)) / 1000)
              + (int) Math.floor(Math.round(1000 * (Math.floor(Lo / delta) - delta * Lo) / (1 + delta)) / 1000)
              + 1;
      int dreiEckLu = (int) Math.floor(Math.round(1000 * (Lu - delta * Math.floor(Lu / delta)) / (1 + delta)) / 1000)
              + (int) Math.floor(Math.round(1000 * (Math.floor(Lu / delta) - delta * Lu) / (1 + delta)) / 1000)
              + 1;
      int minLength = (int) Math.floor(Lo / dreiEckLo);
      int maxLength = (int) Math.ceil(Lu / dreiEckLu);
      for (int x = 0; x < absS - minLength + 1; x++) {
        maxLength = (int) Math.min(maxLength, absS - x);
        for (int lilS = minLength; lilS <= maxLength; lilS++) {
          if (lilS != 0) {
            int[] signatur = Arrays.copyOfRange(r, x, x + lilS);
            outKey.set(signatur, 1);
            outValue.set(1, x, (int)absS, rID, filterUnitR);
//            System.out.println(outKey + " " + outValue);
            context.write(outKey, outValue);
          }
        }
      } 
    }
  }
  
  
  public static class CandidateCreationReducer extends Reducer<MassJoinSignatureKey, MassJoinIntermediateElem, IntWritable, Text> {
    private final IntWritable outKey = new IntWritable();
    private final Text outValue = new Text();
    private double delta;
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      delta = Double.parseDouble(context.getConfiguration().get("delta"));
    }

    @Override
    public void reduce(MassJoinSignatureKey key, Iterable<MassJoinIntermediateElem> values, Context context) throws IOException, InterruptedException {
      // divide input into R and S: R comes first (assured by sorting of second key):
      Iterator<MassJoinIntermediateElem> it = values.iterator();
      ArrayList<MassJoinIntermediateElem> r = new ArrayList();
//      ArrayList<MassJoinIntermediateElem> s = new ArrayList();
      ArrayList<Integer> ridList = new ArrayList();
      while (it.hasNext()) {
        MassJoinIntermediateElem current = it.next();
        if (current.getPartition() == 0) {
          try {
            r.add(current.clone());
          } catch (CloneNotSupportedException ex) {
            Logger.getLogger(Step1SignatureCreationRSJob.class.getName()).log(Level.SEVERE, null, ex);
          }
        } else {
          // we don't need to consider s records if there is no r record:
          if (r.isEmpty()) {
            return;
          }

          double absS = current.getAbs();
          int xi = current.getSegment();

          for (MassJoinIntermediateElem currentR : r) {
            if (currentR.getId() >= current.getId()) {
              continue;
            }
            int i = currentR.getSegment(); 
            double absR = currentR.getAbs();
            int[] currentSFilter = current.getFilterTokens();
            int[] currentRFilter = currentR.getFilterTokens();

            double U = Math.floor(Math.round(1000 * (absR - delta * absS) / (1 + delta)) / 1000)
                    + Math.floor(Math.round(1000 * (absS - delta * absR) / (1 + delta)) / 1000);
            double sum = 0;
            for (int index = 0; index < currentSFilter.length; index++) {
              sum += Math.abs(currentSFilter[index] - currentRFilter[index]);
            }
            if (sum > U) {
              continue;
            }
            int dreiEckAbsR = (int) Math.floor(Math.round(1000 * (absR - delta * Math.floor(absR / delta)) / (1 + delta)) / 1000)
                    + (int) Math.floor(Math.round(1000 * (Math.floor(absR / delta) - delta * absR) / (1 + delta)) / 1000)
                    + 1;
            int kleinere = (int) Math.floor(absR / (dreiEckAbsR));
            int k = (int) (absR - Math.floor(absR / (dreiEckAbsR)) * (dreiEckAbsR));
            int zusatz = 0;
            if (i - dreiEckAbsR + k >= 0) {
              zusatz = i - dreiEckAbsR + k;
            }
            int pil = (int) ((i) * kleinere + zusatz);
            double Usr = Math.floor((absS - delta * absR) / (1 + delta));
            double Urs = Math.floor((absR - delta * absS) / (1 + delta));
            double Xo = Math.max(pil - (i), pil - Urs);
            double Xu = Math.min(pil + absS - absR + (dreiEckAbsR - i), pil + Usr);
            double Lo = Math.ceil(delta * absS);
            double Lu = (Math.floor(absS / delta));
            if (Lo <= absR && absR <= Lu && Xo <= xi && xi <= Xu) {
              ridList.add(currentR.getId());
            }
          }


          if (ridList.size() > 0) {
            String outArr = "";
            for (Integer rt : ridList) { // <<<<<<<<<<<<<<<<<<<<<<<< spielt die Reihenfolge hier eine Rolle? Die ist anders als vorher!
              if (!outArr.equals("")) {
                outArr += "_" + rt;
              } else {
                outArr += rt;
              }
            }
            if (!outArr.isEmpty()) {
              outArr += " 1"; // we need this in order to distinguish raw input from processed input in the next MR step
              outKey.set(current.getId());
              outValue.set(outArr);
              context.write(outKey, outValue);
            }
          }
          ridList.clear();


          }
        }
    }
  }
}
