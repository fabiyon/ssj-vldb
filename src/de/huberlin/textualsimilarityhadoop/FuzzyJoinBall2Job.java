package de.huberlin.textualsimilarityhadoop;

import de.huberlin.utils.BallHashingHamming;
import de.huberlin.utils.BallHashingJaccard;
import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class FuzzyJoinBall2Job extends Configured implements Tool {
  private static final Logger log = Logger.getLogger(FuzzyJoinBall2Job.class);

  public static class StringElem implements WritableComparable<StringElem> {
    private int[] tokens;
    private long partitionID = -1;
    private long windowID = -1;
    private long key;
    private long prevPartition = -1;

    StringElem() {}
    
    StringElem(String text) {
      String[] textArr = text.split("\\s+");
      if (textArr.length > 0) {
        key = Long.parseLong(textArr[0]);
      }
      if (textArr.length > 1) {
        tokens = this.toTokenArr(textArr[1]);
      } else {
        tokens = new int[0];
      }
      if (textArr.length > 2) { // the last element resembles the prevPartition:
        prevPartition = Long.parseLong(textArr[2]);
      }
    }
    
    StringElem(String key, String tokens) {
      this.key = Long.parseLong(key);
      this.tokens = this.toTokenArr(tokens);
    }
    
    StringElem(long key, int[] tokens) {
      this.key = key;
      this.tokens = tokens;
    }
    
    // copy constructor:
    StringElem(StringElem elem) {
      this.key = elem.getKey();
      this.tokens = Arrays.copyOf(elem.getTokens(), elem.getTokens().length);
    }
    
    public int[] getTokens() {
      return this.tokens;
    }
    
    private int[] toTokenArr(String input) {
      String[] tokenStringArr = input.split(",");
      int[] tokenArr = new int[tokenStringArr.length];
      int count = 0;
      for (String token : tokenStringArr) {
        if (!token.equals("")) {
          tokenArr[count++] = Integer.parseInt(token);
        }
      }
      return tokenArr;
    }

    public int getSizeInBytes() { 
      return 32 + // 4 long variables à 64 bits
              tokens.length * 4;// + // tokens.length 32 bits = 4 bytes
    }

    public long getKey() {
      return key;
    }

    public void setPartitionID(long partitionID) {
      this.partitionID = partitionID;
    }

    public long getPartitionID() {
      return partitionID;
    }

    public long getWindowID() {
      return windowID;
    }

    public void setWindowID(long windowID) {
      this.windowID = windowID;
    }

    public long getPrevPartition() {
      return prevPartition;
    }

    public void setPrevPartition(long prevPartition) {
      this.prevPartition = prevPartition;
    }

    public double getDistanceBetween(StringElem o) {
      double intersection = 0;
      if (this.tokens != null && o.tokens != null) {
        for (int i = 0; i < this.tokens.length; i++) {
          for (int j = 0; j < o.tokens.length; j++) {
            if (o.tokens[j] == this.tokens[i]) {
              intersection++;
            }
          }
        }
        return 1 - (double) (intersection / (this.tokens.length + o.tokens.length - intersection)); 
      }
      return 1;
    }

    // readFields() and write() are used within Hadoop to serialize and unserialize the objects
    @Override
    public void readFields(DataInput in) throws IOException {
      partitionID = in.readLong();
      windowID = in.readLong();
      key = in.readLong();
      tokens = new int[in.readInt()];
      for (int i = 0; i < tokens.length; i++) {
        tokens[i] = in.readInt();
      }
      prevPartition = in.readLong();
    }
    
    private String getTokensAsString() {
      String outString = "";
      for (int i = 0; i < tokens.length; i++) {
        if (!outString.equals("")) {
          outString += ",";
        }
        outString += tokens[i];
      }
      return outString;
    }

    @Override
    public void write(DataOutput out) throws IOException {
      out.writeLong(partitionID);
      out.writeLong(windowID);
      out.writeLong(key);
      out.writeInt(tokens.length);
      for (int i = 0; i < tokens.length; i++) {
        out.writeInt(tokens[i]);
      }
      out.writeLong(prevPartition);
    }

    @Override
    public int compareTo(StringElem o) {
      long thisValue = this.key;
      long thatValue = o.key;
      return (thisValue < thatValue ? -1 : (thisValue==thatValue ? 0 : 1));
    }

    @Override
    public String toString() {
      return key + " " + getTokensAsString();
    }

    public String toStringPart() {
      return this.toString() + " " + partitionID;
    }

    public String toStringPrev() {
      return this.toString() + " " + prevPartition;
    }
  }// end StringElem

  public static class TokenKey implements WritableComparable<TokenKey> {
    private int[] tokens;
    
    // this constructor is needed by Hadoop
    TokenKey() {}

    TokenKey(int[] tokens) {
      this.tokens = tokens;
    }
    
    public int[] getTokens() {
      return this.tokens;
    }

    public double getDistanceBetween(TokenKey o) {
      double intersection = 0;
      if (this.tokens != null && o.tokens != null) {
        for (int i = 0; i < this.tokens.length; i++) {
          for (int j = 0; j < o.tokens.length; j++) {
            if (o.tokens[j] == this.tokens[i]) {
              intersection++;
            }
          }
        }
        return 1 - (double) (intersection / (this.tokens.length + o.tokens.length - intersection)); 
      }
      return 1;
    }

    // readFields() and write() are used within Hadoop to serialize and unserialize the objects
    @Override
    public void readFields(DataInput in) throws IOException {
      tokens = new int[in.readInt()];
      for (int i = 0; i < tokens.length; i++) {
        tokens[i] = in.readInt();
      }
    }

    @Override
    public void write(DataOutput out) throws IOException {
      out.writeInt(tokens.length);
      for (int i = 0; i < tokens.length; i++) {
        out.writeInt(tokens[i]);
      }
    }

    @Override
    public int compareTo(TokenKey o) {
      if (tokens.length < o.getTokens().length) {
        return -1;
      } else if (tokens.length > o.getTokens().length) {
        return 1;
      } 
      // check for equality token by token:
      for (int i = 0; i < tokens.length; i++) {
        if (tokens[i] < o.getTokens()[i]) {
          return -1;
        } else if (tokens[i] > o.getTokens()[i]) {
          return 1;
        }
      }
      return 0;
    }

    @Override
    public String toString() {
      String outString = "";
      for (int i = 0; i < tokens.length; i++) {
        if (!outString.equals("")) {
          outString += ",";
        }
        outString += tokens[i];
      }
      return outString;
    }
  }// end TokenKey

  // =============== PREPROCESSING 1: Compute Dictionary =========================
  
  public static class UniqueTokensMapper extends Mapper<Object, Text, IntWritable, IntWritable> {
    private final IntWritable outKey = new IntWritable();
    private final IntWritable outValue = new IntWritable(1);

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String[] valueArr = value.toString().split("\\s+");
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
  
  public static class UniqueTokensCombiner extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
    private final IntWritable outVal = new IntWritable(1);

    @Override
    public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      context.write(key, outVal);
    }
  }
  
  public static class UniqueTokensReducer extends Reducer<IntWritable, IntWritable, IntWritable, NullWritable> {
    private final IntWritable outKey = new IntWritable();
    private final NullWritable outVal = NullWritable.get();

    @Override
    public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      context.write(key, outVal);
    }
  }
  
  // =============== APPLICATION OF BALL HASHING ========================
  public static class ReplicationMapper extends Mapper<LongWritable, Text, TokenKey, StringElem> {//IntWritable, IntWritable> {
    StringElem outKey = new StringElem();
    double theta;
    BallHashingHamming bhh;
  
    @Override
    public void setup(Context context) throws FileNotFoundException, IOException {      
      ArrayList<Integer> tmpArrList = new ArrayList();
      try {
        Path pt = new Path(context.getConfiguration().get("tokenFrequencyPath") + "/part-r-00000");
        FileSystem fs = FileSystem.get(new Configuration());
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
        String line = br.readLine();
        while (line != null) {
          tmpArrList.add(Integer.parseInt(line));
          line = br.readLine();
        }
      } catch(Exception e) {
      }
      
      // change the ArrayList to intArr
      int[] universe = new int[tmpArrList.size()];
      for (int i = 0; i < tmpArrList.size(); i++) {
        universe[i] = tmpArrList.get(i);
      }
      
      // lies den Schwellwert:
      double thetaOriginal = context.getConfiguration().getDouble("theta", 0);
      theta = 1.0 - ((1.0 - thetaOriginal) / 2);
      bhh = new BallHashingHamming(theta, universe);
    }

    @Override
    public void map(LongWritable inputKey, Text inputValue, Context context) throws IOException, InterruptedException {
      StringElem currentElement;
      try {
        currentElement = new StringElem(inputValue.toString());
        if (currentElement.getKey() == 1 || currentElement.getKey() == 2) {
          System.out.println();
        }
      } catch (NumberFormatException e) {
        System.out.println(e);
        return;
      }

      // output for each ball element:
      int[][] ball = bhh.computeBall(currentElement.getTokens());
      for (int[] currentRecord : ball) {
        context.write(new TokenKey(currentRecord), currentElement);
      }
    }
  }

  public static class VerificationReducer extends Reducer<TokenKey, StringElem, LongWritable, LongWritable> {
    LongWritable outKey = new LongWritable();
    LongWritable outValue = new LongWritable();
    double theta;
    BallHashingHamming bhh;
  
    @Override
    public void setup(Context context) {
      theta = context.getConfiguration().getDouble("theta", 0);
      ArrayList<Integer> tmpArrList = new ArrayList();
      try {
        Path pt = new Path(context.getConfiguration().get("tokenFrequencyPath") + "/part-r-00000");
        FileSystem fs = FileSystem.get(new Configuration());
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
        String line = br.readLine();
        while (line != null) {
          tmpArrList.add(Integer.parseInt(line));
          line = br.readLine();
        }
      } catch(Exception e) {
      }
      
      // change the ArrayList to intArr
      int[] universe = new int[tmpArrList.size()];
      for (int i = 0; i < tmpArrList.size(); i++) {
        universe[i] = tmpArrList.get(i);
      }
      bhh = new BallHashingHamming(theta, universe);
    }
    
    @Override
    public void reduce(TokenKey key, Iterable<StringElem> values, Context context) throws IOException, InterruptedException {
      Iterator<StringElem> it = values.iterator();

      ArrayList<StringElem> readRecords = new ArrayList();
      // Simple NLJ:
      while (it.hasNext()) {
        StringElem currentElement = it.next();
        for (StringElem compareElement : readRecords) {
          double distance = compareElement.getDistanceBetween(currentElement);
          if (distance <= (1 - theta) && compareElement.getKey() < currentElement.getKey()) {
            // filter out duplicates. Idea: only output this pair if the key of this partition is equal to the lexicographically first signature at distance d/2:
            // 0. generate this lexicographically first signature:
            int[] sig1 = bhh.getLexicographicallyFirstSignature(compareElement.tokens);
            int[] sig2 = bhh.getLexicographicallyFirstSignature(currentElement.tokens);
            
            if (Arrays.equals(sig1, sig2) && Arrays.equals(sig2, key.tokens)) {
              outKey.set(compareElement.getKey());
              outValue.set(currentElement.getKey());
              context.write(outKey, outValue);
            }
          }
        }
        readRecords.add(new StringElem(currentElement));
      }
    }//end reduce	
  }//end VerificationReducer
  
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new FuzzyJoinBall2Job(), args);
  }

  @Override
  public int run(String[] args) throws Exception {
    System.out.println("FuzzyJoinBall2");
    ParameterParser pp = new ParameterParser(args);
    double theta = Double.parseDouble(pp.getTheta());
    
    String inputPathString = pp.getInput();
    String outputPathString = pp.getOutput();
    log.setLevel(Level.ALL);

    Configuration conf = new Configuration();
    conf.setDouble("theta", theta);
    
//    String recordLengthPath = outputPathString + "recordlength";
    
    
    String tokenPath = outputPathString + "tokens";
    
    FileSystem filesystem = FileSystem.get(getConf());
    filesystem.delete(new Path(tokenPath), true);
    Path outputFile = new Path(outputPathString);
    filesystem.delete(outputFile, true);
    conf.set("tokenFrequencyPath", tokenPath);
//    conf.set("recordLengthPath", recordLengthPath);
    
    Job job0 = Job.getInstance(conf, "FuzzyJoinBall2 Preprocessing");
    job0.setJarByClass(FuzzyJoinBall2Job.class);

    
    Path tokenFile = new Path(tokenPath);
    FileInputFormat.addInputPath(job0, new Path(inputPathString));
    FileOutputFormat.setOutputPath(job0, tokenFile);
    
    job0.setMapperClass(UniqueTokensMapper.class);
    job0.setMapOutputKeyClass(IntWritable.class);
    job0.setMapOutputValueClass(IntWritable.class);
    
    job0.setCombinerClass(UniqueTokensCombiner.class);

    job0.setReducerClass(UniqueTokensReducer.class);
    job0.setOutputKeyClass(IntWritable.class);
    job0.setOutputValueClass(NullWritable.class);
    
    job0.setNumReduceTasks(1);

    job0.waitForCompletion(true);

    Job job = Job.getInstance(conf, "FuzzyJoinBall2 Replication and Join");
    job.setJarByClass(FuzzyJoinBall2Job.class);

    FileInputFormat.addInputPath(job, new Path(inputPathString));
    FileOutputFormat.setOutputPath(job, outputFile);
    
    job.setMapperClass(ReplicationMapper.class);
    job.setMapOutputKeyClass(TokenKey.class);
    job.setMapOutputValueClass(StringElem.class);

    job.setReducerClass(VerificationReducer.class);
    job.setOutputKeyClass(LongWritable.class);
    job.setOutputValueClass(LongWritable.class);
    
    job.waitForCompletion(true);
    
    return 0;
  }
  
}
