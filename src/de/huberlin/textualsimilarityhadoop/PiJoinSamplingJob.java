package de.huberlin.textualsimilarityhadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
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

public class PiJoinSamplingJob extends Configured implements Tool {
  private static final Logger log = Logger.getLogger(PiJoinSamplingJob.class);
  
  public static class GroupJoinTokens  {
    private int[] tokens;
    
    public GroupJoinTokens() {
      tokens = new int[0];
    }
    
    private void setTokens(String input) {
      String[] tokenStringArr = input.split(",");
      tokens = new int[tokenStringArr.length];
      int count = 0;
      for (String token : tokenStringArr) {
        if (!token.equals("")) {
          tokens[count++] = Integer.parseInt(token);
        }
      }
    }

    public double getJaccardSimilarityBetween(GroupJoinTokens o) {
      double intersection = 0;
      if (this.tokens != null && o.tokens != null) {
        for (int i = 0; i < this.tokens.length; i++) {
          for (int j = 0; j < o.tokens.length; j++) {
            if (o.tokens[j] == this.tokens[i]) {
              intersection++;
            }
          }
        }
        return (double) (intersection / (this.tokens.length + o.tokens.length - intersection)); 
      }
      return 0;
    }

    // readFields() and write() are used within Hadoop to serialize and unserialize the objects
    public void readFields(DataInput in) throws IOException {
      tokens = new int[in.readInt()];
      for (int i = 0; i < tokens.length; i++) {
        tokens[i] = in.readInt();
      }
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

    public void write(DataOutput out) throws IOException {
      out.writeInt(tokens.length);
      for (int i = 0; i < tokens.length; i++) {
        out.writeInt(tokens[i]);
      }
    }

    public int compareTo(GroupJoinTokens o) {
      if (tokens.length < o.tokens.length) {
        return -1;
      } else if (tokens.length > o.tokens.length) {
        return 1;
      } 
      // check for equality token by token:
      for (int i = 0; i < tokens.length; i++) {
        if (tokens[i] < o.tokens[i]) {
          return -1;
        } else if (tokens[i] > o.tokens[i]) {
          return 1;
        }
      }
      return 0;
    }

    @Override
    public String toString() {
      return getTokensAsString();
    }
  }
  
  public static class GroupJoinKey extends GroupJoinTokens implements WritableComparable<GroupJoinKey> {
    private int i;
    private int l;
    
    GroupJoinKey() {}
    
    public GroupJoinKey(int i, int l) {
      this.i = i;
      this.l = l;
    }
    
    public void addToken(int token) {
      super.tokens = Arrays.copyOf(super.tokens, super.tokens.length + 1);
      super.tokens[super.tokens.length - 1] = token;
    }
    
    public int getLength() {
      return super.tokens.length;
    }
    
    public int getI() {
      return i;
    }
    
    public void setI(int i) {
      this.i = i;
    }
    
    public int getL() {
      return l;
    }
    
    public void setL(int l) {
      this.l = l;
    }
    
    @Override
    public void readFields(DataInput in) throws IOException {
      i = in.readInt();
      l = in.readInt();
      super.readFields(in);
    }
    
    @Override
    public void write(DataOutput out) throws IOException {
      out.writeInt(i);
      out.writeInt(l);
      super.write(out);
    }
    
    @Override
    public int compareTo(GroupJoinKey o) {
      if (this.l < o.l) {
        return -1;
      } else if (this.l > o.l) {
        return 1;
      } else {
        if (this.i < o.i) {
          return -1;
        } else if (this.i > o.i) {
          return 1;
        } else {
          return super.compareTo(o);
        }
      }
    }
    
    public static class GroupJoinPartitioner extends Partitioner<GroupJoinKey, GroupJoinVal> implements Configurable {
      private Configuration configuration;

      @Override
      public int getPartition(GroupJoinKey key, GroupJoinVal value, int numPartitions) {
        int result = (int)((key.l + key.i) % numPartitions);
        return result; // should work... Not sure if its a really good idea...
      }

      @Override
      public void setConf(Configuration conf) {
        this.configuration = conf;
      }

      @Override
      public Configuration getConf() {
        return configuration;
      }
    } // end of GroupJoinPartitioner
   
  }
  
  public static class GroupJoinVal extends GroupJoinTokens implements WritableComparable<GroupJoinVal> {
    private int rid;
    private int type = 0; // 0: index, 1: probe
    private String rawData = "";
    
    GroupJoinVal() {}
    
    public GroupJoinVal(int rid, int type, int[] tokens, String rawData) {
      this.rid = rid;
      this.type = type;
      this.rawData = rawData;
      super.tokens = tokens;
    }
    
    public boolean set(String text, boolean joinRaw) {
      type = 0; // reset to default
      String[] textArr = text.split("\\s+"); // format rid token1,token2,...
      this.rid = Integer.parseInt(textArr[0]);
      if (textArr.length < 2) {
        return false;
      }
      super.setTokens(textArr[1]);
      if (joinRaw) {
        for (int i = 2; i < textArr.length; i++) {
          rawData += " " + textArr[i];
        }
      }
      return true;
    }
    
    public int getType() {
      return type;
    }
    
    public void setType(int type) {
      this.type = type;
    }
    
    public int getLength() {
      return super.tokens.length;
    }
    
    public int getRid() {
      return rid;
    }
    
    public double getJaccardSimilarityBetween(GroupJoinVal o) {
      if (this.rid != o.rid) {
        return super.getJaccardSimilarityBetween(o);
      }
      return 0;
    }
    
    public GroupJoinKey[] getIndexKeysFor(double delta, int length) {
      int numberOfPartitions = (int)Math.floor(((1 - delta) / delta) * (double)length) + 1;
      GroupJoinKey[] keys = new GroupJoinKey[numberOfPartitions];
      for (int i = 0; i < numberOfPartitions; i++) { // Partitions-IDs fangen bei 0 an
        keys[i] = new GroupJoinKey(i, length);
      }
      
      for (int token : super.tokens) {
        int group = token % numberOfPartitions;
        keys[group].addToken(token);
      }
      
      return keys;
    }
    
    public GroupJoinKey[] getProbeKeysFor(double delta) {
      // unteren Schwellwert (kleinste Länge) berechnen:
      int lowestLength = (int)Math.ceil(delta * this.getLength());
      GroupJoinKey[][] tmp = new GroupJoinKey[this.getLength() - lowestLength + 1][];
      int i = 0;
      int sumOfGJK = 0;
      for (int s = lowestLength; s <= this.getLength(); s++) {
        tmp[i] = getIndexKeysFor(delta, s);
        sumOfGJK += tmp[i++].length;
      }
      
      GroupJoinKey[] keys = new GroupJoinKey[sumOfGJK];
      
      i = 0;
      for (GroupJoinKey[] keysTmp : tmp) {
        for (GroupJoinKey key : keysTmp) {
          keys[i++] = key;
        }
      }
      
      return keys;
    }
    
    @Override
    public void readFields(DataInput in) throws IOException {
      rid = in.readInt();
      type = in.readInt();
      rawData = in.readUTF();
      super.readFields(in);
    }
    
    @Override
    public void write(DataOutput out) throws IOException {
      out.writeInt(rid);
      out.writeInt(type);
      out.writeUTF(rawData);
      super.write(out);
    }
    
    @Override
    public int compareTo(GroupJoinVal o) {
      int thisValue = this.rid;
      int thatValue = o.rid;
      return (thisValue < thatValue ? -1 : (thisValue==thatValue ? 0 : 1));
    }
    
    @Override
    public GroupJoinVal clone() {
      return new GroupJoinVal(rid, type, Arrays.copyOf(super.tokens, super.tokens.length), rawData);
    }
    
    @Override
    public String toString() {
      if (this.rawData.isEmpty()) {
        return super.toString();
      }
      return this.rawData;
    }
  }
  
  public static class GroupJoinMapper extends Mapper<LongWritable, Text, GroupJoinKey, GroupJoinVal> {
    public double delta;
    public boolean joinRaw = false;
    private final GroupJoinVal val = new GroupJoinVal();
   
    @Override
    public void map(LongWritable inputKey, Text inputValue, Context context) throws IOException, InterruptedException {
      if (val.set(inputValue.toString(), joinRaw)) { // we can only compute valid records

        // generiere die korrespondierenden Index-Partitionen
        GroupJoinKey[] keys = val.getIndexKeysFor(delta, val.getLength());
        for (GroupJoinKey key : keys) {
          if (key.getLength() != 0) {
            context.write(key, val);
          }
        }

        // generiere die korrespondierenden Probe-Partitionen
        val.setType(1);
        keys = val.getProbeKeysFor(delta);
        for (GroupJoinKey key : keys) {
          if (key.getLength() != 0) {
            context.write(key, val);
          }
        }
      }
    }
    
    @Override
    public void setup(Context context) {
      delta = context.getConfiguration().getDouble("delta", 0);
      String inputDataType = context.getConfiguration().get("inputDataType", "");
      if (inputDataType.equals(DetectInputDataType.DataType.RAW.toString()) || 
              inputDataType.equals(DetectInputDataType.DataType.STRINGANDINTEGERTOKENIZED.toString())) {
        joinRaw = true;
      }
    }
  }
  
  public static class GroupJoinReducer extends Reducer<GroupJoinKey, GroupJoinVal, Text, NullWritable> {
    Text outKey = new Text();
    NullWritable outVal = NullWritable.get();
    public double delta;
    public boolean joinRaw = false;
    
    @Override
    public void reduce(GroupJoinKey key, Iterable<GroupJoinVal> values, Context context) throws IOException, InterruptedException {
//      if (key.getL() == 4 && key.getI() == 0) {
//        System.out.println();
//      }
      // Generiere temporäre Listen von Index und Probe:
      ArrayList<GroupJoinVal> indexList = new ArrayList();
      ArrayList<GroupJoinVal> probeList = new ArrayList();

      Iterator<GroupJoinVal> it = values.iterator();
      GroupJoinVal currentVal;
      while (it.hasNext()) {
        currentVal = it.next();
        if (currentVal.getType() == 0) {
          indexList.add(currentVal.clone()); // CLONE!!!
        } else {
          probeList.add(currentVal.clone()); // CLONE!!!
        }
      }
      
      for (GroupJoinVal currentVal1 : probeList) {
        for (GroupJoinVal indexVal : indexList) {
          // Falls die Länge beider Records gleich ist, landen sie in der Probe- und Indexliste. Das führt zu Duplikaten. Wir nehmen in dem Fall einfach nur eines der Paare:
          if (currentVal1.getLength() == indexVal.getLength() && currentVal1.getRid() > indexVal.getRid()) {
            continue;
          }
//          if ((currentVal1.getRid() == 8202 && indexVal.getRid() == 8358) || 
//                  (currentVal1.getRid() == 8358 && indexVal.getRid() == 8202)) {
//            System.out.println();
//          }
          if (currentVal1.getJaccardSimilarityBetween(indexVal) > delta) {
            int i = key.getI();
            boolean doWrite = true; // <<<<< GIBTS ÜBERHAUPT EINE PARTITIONS-ID 0? Ja. Aber es scheint einen Sonderfall bei l=1 zu geben
            if (i > 0) { // wenn die Partitions-ID 0 ist, dann kann es keine Partition davor geben, die gleich war
              // check if this result pair has not beed considered before:
              // we keep l static and look for all j < the current i.
              int l = key.getL();
              GroupJoinKey[] previousIndexKeys = indexVal.getIndexKeysFor(delta, l);
//              GroupJoinKey[] previousProbeKeys = currentVal1.getIndexKeysFor(delta, l);// INDEX KEY!!!
              GroupJoinKey[] previousProbeKeys = currentVal1.getProbeKeysFor(delta);
              
              // is there an identical key for j < i?
              for (int j = 0; j < i; j++) { // zusätzliche Bedingung: die Partition darf nicht leer sein:
                // jetzt ist sie doppelt...
                if (previousIndexKeys[j].compareTo(previousProbeKeys[j]) == 0 && previousIndexKeys[j].getLength() != 0) {
                  doWrite = false;
                  break;
                }
              }
            }
            if (doWrite) {
              if (joinRaw) {
                context.write(new Text(currentVal1.toString() + " " + indexVal.toString()), outVal);
              } else {
                context.write(new Text(currentVal1.getRid() + " " + indexVal.getRid()), outVal);
              }
            }
          }
        }
      }
    }
    
    @Override
    public void setup(Context context) {
      delta = context.getConfiguration().getDouble("delta", 0);
      String inputDataType = context.getConfiguration().get("inputDataType", "");
      if (inputDataType.equals(DetectInputDataType.DataType.RAW.toString()) || 
              inputDataType.equals(DetectInputDataType.DataType.STRINGANDINTEGERTOKENIZED.toString())) {
        joinRaw = true;
      }
    }
  }
  
  @Override
  public int run(String[] args) throws Exception {
    System.out.println("PiJoinSampling");
//    log.setLevel(Level.ALL);
    ParameterParser pp = new ParameterParser(args);
    
    Configuration conf = this.getConf();
    conf.set("delta", pp.getTheta());
    String inputPathString = pp.getInput();
    Path inputPath = new Path(inputPathString);
    String outputPath = pp.getOutput();
    FileSystem filesystem = FileSystem.get(getConf());
    filesystem.delete(new Path(outputPath), true);
    
    System.out.println("Step 1: Filter and Verify (without duplicates)");
    Job job1a = Job.getInstance(conf, "Step 1: Filter and Verify (without duplicates)");
    job1a.setJarByClass(PiJoinSamplingJob.class);
    
    job1a.setMapperClass(GroupJoinMapper.class);
    job1a.setMapOutputKeyClass(GroupJoinKey.class);
    job1a.setMapOutputValueClass(GroupJoinVal.class);
    
    job1a.setReducerClass(GroupJoinReducer.class);
    job1a.setOutputKeyClass(Text.class);
    job1a.setOutputValueClass(NullWritable.class);
    
    job1a.setPartitionerClass(GroupJoinKey.GroupJoinPartitioner.class); // this is only called if there is more than 1 reducer configured!
    
    if (job1a.getNumReduceTasks() == 1) {
      job1a.setNumReduceTasks(2);
    }
    
    FileInputFormat.addInputPath(job1a, inputPath);
    FileOutputFormat.setOutputPath(job1a, new Path(outputPath));
    int returnCode = job1a.waitForCompletion(true) ? 0 : 1;

    return returnCode;
  }
  
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new Configuration(), new PiJoinSamplingJob(), args);
  }
  
}