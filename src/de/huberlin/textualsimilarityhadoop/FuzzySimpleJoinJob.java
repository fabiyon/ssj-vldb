package de.huberlin.textualsimilarityhadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
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

public class FuzzySimpleJoinJob extends Configured implements Tool {
  private static final Logger log = Logger.getLogger(FuzzySimpleJoinJob.class);

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


  public static class PartitioningMapper extends Mapper<LongWritable, Text, IntWritable, StringElem> {
    IntWritable outKey = new IntWritable();
    double theta;
  
    @Override
    public void setup(Context context) throws FileNotFoundException, IOException {
      // lies den Schwellwert:
      theta = context.getConfiguration().getDouble("theta", 0);
    }

    @Override
    public void map(LongWritable inputKey, Text inputValue, Context context) throws IOException, InterruptedException {
      // Länge herausfinden:
      StringElem currentElement;
      try {
        currentElement = new StringElem(inputValue.toString());
      } catch (NumberFormatException e) {
        System.out.println(e);
        return;
      }
      int length = currentElement.getTokens().length;
      
      // Obergrenze berechnen: =ABRUNDEN(B1/0,7)
      int upperBound = (int)Math.floor((double)length / theta);
      
      // Untergrenze berechnen: =MIN(AUFRUNDEN(B1*0,7)+1;B3)
      int lowerBound = Math.min((int)Math.ceil((double)length * theta) + 1, upperBound);
      
      for (int i = lowerBound; i <= upperBound; i++) {
        outKey.set(i);
        context.write(outKey, currentElement);
      }
    }
  }

  public static class VerificationReducer extends Reducer<IntWritable, StringElem, LongWritable, LongWritable> {
    LongWritable outKey = new LongWritable();
    LongWritable outValue = new LongWritable();
    double eps;
    double theta;
  
    @Override
    public void setup(Context context) {
      theta = context.getConfiguration().getDouble("theta", 0);
      eps = 1 - theta;
    }
    
    @Override
    public void reduce(IntWritable key, Iterable<StringElem> values, Context context) throws IOException, InterruptedException {
      Iterator<StringElem> it = values.iterator();
//      int keyInt = key.get();
      
      // Perform NLJ (es fehlen Resultate, wahrscheinlich weil der Ansatz mit den Basis-Elementen so nicht stimmt...)
//      ArrayList<StringElem> readRecords = new ArrayList();
//      ArrayList<StringElem> baseElements = new ArrayList();
//      while (it.hasNext()) {
//        StringElem currentElement = it.next();
//        int length = currentElement.getTokens().length;
//        int upperBound = (int)Math.floor((double)length / theta);
//        
//        if (upperBound == keyInt) {
//          baseElements.add(new StringElem(currentElement));
//        }
//        readRecords.add(new StringElem(currentElement)); // wir müssen die immer in die baseRecords einfügen, weil es natürlich auch gleich lange Records gibt, die miteinander gejoint werden müssen
//        // eine andere Variante wäre, die baseElements untereinander separat zu joinen
//      }
//      
//      for (StringElem baseElement : baseElements) {
//        for (StringElem compareElement : readRecords) {
//          double distance = compareElement.getDistanceBetween(baseElement);
//          if (distance <= eps && compareElement.getKey() < baseElement.getKey()) {
//            outKey.set(compareElement.getKey());
//            outValue.set(baseElement.getKey());
//            context.write(outKey, outValue);
//          }
//        }
//      }
      
      // das ist jetzt die naive Lösung, die Duplikate generiert:
      ArrayList<StringElem> readRecords = new ArrayList();
      while (it.hasNext()) {
        StringElem currentElement = it.next();
        
        for (StringElem compareElement : readRecords) {
          double distance = compareElement.getDistanceBetween(currentElement);
          if (distance <= eps && compareElement.getKey() != currentElement.getKey()) { // < reicht nicht aus bei den Keys. Wahrscheinlich weil die Records nicht sortiert sind
            outKey.set(compareElement.getKey());
            outValue.set(currentElement.getKey());
            context.write(outKey, outValue);
          }
        }

        readRecords.add(new StringElem(currentElement)); 
      }
      

    }//end reduce	
  }//end VerificationReducer
  
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new FuzzySimpleJoinJob(), args);
  }

  @Override
  public int run(String[] args) throws Exception {
    System.out.println("FuzzySimpleJoin");
//    ParameterParser pp = new ParameterParser(args);
//    double theta = Double.parseDouble(pp.getTheta());
//    
//    String inputPathString = pp.getInput();
//    String outputPathString = pp.getOutput();
//    log.setLevel(Level.ALL);
//
//    Configuration conf = new Configuration();
//    conf.setDouble("theta", theta);
//    
//    Job job = Job.getInstance(conf, "FuzzySimpleJoin Partitioning by length, verification");
//    job.setJarByClass(FuzzySimpleJoinJob.class);
//
//    Path intermediateFile = new Path(outputPathString + "1");
//    FileInputFormat.addInputPath(job, new Path(inputPathString));
//    FileOutputFormat.setOutputPath(job, intermediateFile);
//    
//    job.setMapperClass(PartitioningMapper.class);
//    job.setMapOutputKeyClass(IntWritable.class);
//    job.setMapOutputValueClass(StringElem.class);
//
//    job.setReducerClass(VerificationReducer.class);
//    job.setOutputKeyClass(LongWritable.class);
//    job.setOutputValueClass(LongWritable.class);
//
//    job.waitForCompletion(true);
//    
//    
//    Job job2 = Job.getInstance(conf, "De-Duplication");
//    job2.setJarByClass(VernicaDeduplicationJob.class);
//    
//    job2.setMapperClass(VernicaDeduplicationJob.DedupMapper.class);
//    job2.setMapOutputKeyClass(LongWritable.class);
//    job2.setMapOutputValueClass(LongWritable.class);
//    
//    job2.setReducerClass(VernicaDeduplicationJob.DedupReducer.class);
//    job2.setOutputKeyClass(LongWritable.class);
//    job2.setOutputValueClass(LongWritable.class);
//    
//    FileInputFormat.addInputPath(job2, intermediateFile);
//    FileOutputFormat.setOutputPath(job2, new Path(outputPathString));
//    job2.waitForCompletion(true);
        
    return 0;
  }
  
}
