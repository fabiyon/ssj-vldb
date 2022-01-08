package de.huberlin.clusterjoin;

import de.huberlin.textualsimilarityhadoop.ParameterParser;
import de.huberlin.vernicajoin.VernicaJoinDriver;
import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.lib.InputSampler;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ClusterJoinJob extends Configured implements Tool {

  public static class StringElem implements WritableComparable<StringElem> {
    private int[] tokens;
    private int key;

    StringElem() {}
    
    StringElem(String text) {
      String[] textArr = text.split("\\s+");
      if (textArr.length > 0) {
        key = Integer.parseInt(textArr[0]);
      }
      if (textArr.length > 1) {
        tokens = this.toTokenArr(textArr[1]);
      } else {
        tokens = new int[0];
      }
    }
    
    StringElem(String key, String tokens) {
      this.key = Integer.parseInt(key);
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

    public int getKey() {
      return key;
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
//      partitionID = in.readInt();
      key = in.readInt();
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

    @Override
    public void write(DataOutput out) throws IOException {
      out.writeInt(key);
      out.writeInt(tokens.length);
      for (int i = 0; i < tokens.length; i++) {
        out.writeInt(tokens[i]);
      }
    }

    @Override
    public int compareTo(StringElem o) {
      int thisValue = this.key;
      int thatValue = o.key;
      return (thisValue < thatValue ? -1 : (thisValue==thatValue ? 0 : 1));
    }

    @Override
    public String toString() {
      return key + " " + getTokensAsString();
    }

  }// end StringElem

  public static class ClusterJoinKey implements WritableComparable<ClusterJoinKey> {

    int partitionID = -1;
    int groupId = -1; // enhancement described in the experiments
    boolean isInWindow;
    int col = -1; // this contains the concept of ThetaJoin partitioning
    int row = -1; // this contains the concept of ThetaJoin partitioning

    ClusterJoinKey() {
    }

    ClusterJoinKey(int partitionID) {
      this.partitionID = partitionID;
    }
    
    public void setIsInWindow(boolean isInWindow) {
      this.isInWindow = isInWindow;
    }
    
    public boolean getIsInWindow() {
      return isInWindow;
    }

    public int getPartitionID() {
      return partitionID;
    }

    public void setPartitionID(int partitionID) {
      this.partitionID = partitionID;
    }

    public int getCol() {
      return col;
    }

    public void setCol(int col) {
      this.col = col;
    }
    
    public int getRow() {
      return row;
    }

    public void setRow(int row) {
      this.row = row;
    }
    
    public int getGroupId() {
      return groupId;
    }

    public void setGroupId(int groupId) {
      this.groupId = groupId;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      partitionID = in.readInt();
      groupId = in.readInt();
      row = in.readInt();
      col = in.readInt();
      isInWindow = in.readBoolean();
    }

    @Override
    public void write(DataOutput out) throws IOException {
      out.writeInt(partitionID);
      out.writeInt(groupId);
      out.writeInt(row);
      out.writeInt(col);
      out.writeBoolean(isInWindow);
    }

    // we use a dedicated byte-wise comparator for sorting and grouping.
    // This here would be inefficient.
    @Override
    public int compareTo(ClusterJoinKey o) {
      throw new UnsupportedOperationException("Not supported.");
    }

    @Override
    public String toString() {
//      String window = "";
//      if (isInWindow) {
//        window = " isWindow";
//      }
      return "PID: " + partitionID + " GID: " + groupId + " ROW: " + row + " COL: " + col; //  + window
    }
  }
  
  public static class CardinalityEstimationMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
    IntWritable outKey = new IntWritable(1);

    @Override
    public void map(LongWritable inputKey, Text inputValue, Context context) throws IOException, InterruptedException {
      context.write(outKey, inputValue);
    }
  }

  public static class CardinalityEstimationReducer extends Reducer<IntWritable, Text, LongWritable, Text> {
    int numberOfPivots;
    LongWritable outKey = new LongWritable(0);
  
    @Override
    public void setup(Context context) {
      numberOfPivots = context.getConfiguration().getInt("numberOfPivots", 0);
    }
    
    @Override
    public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException, NullPointerException {
      Iterator<Text> it = values.iterator();
      TreeMap<StringElem, Integer> pivots = new TreeMap();
      int counter = 0;
      while (it.hasNext()) {
        if (counter++ < numberOfPivots) {
          pivots.put(new StringElem(it.next().toString()), 0);
        } else {
          double distance = Double.MAX_VALUE;
          StringElem currentlyAssignedTo = null;
          StringElem currentElement = new StringElem(it.next().toString());
          // find out to which of the pivots it is the closest:
          for(Map.Entry<StringElem,Integer> entry : pivots.entrySet()) {
            double tempDistance = currentElement.getDistanceBetween(entry.getKey());
            if (tempDistance < distance) {
              distance = tempDistance;
              currentlyAssignedTo = entry.getKey();
            }
          }
          
          if (currentlyAssignedTo == null) {
            continue; // This happens when a record only consists of a rID and does not contain any token.
          }
          // increment count of currentlyAssignedTo
          Integer currentCount = pivots.get(currentlyAssignedTo);
          currentCount++; // reicht das (ist ja ein Objekt)?
          pivots.put(currentlyAssignedTo, currentCount);
        }
      }
      
      for(Map.Entry<StringElem,Integer> entry : pivots.entrySet()) {
        context.write(outKey, new Text(entry.getKey().toString() + " " + entry.getValue()));
//        System.out.println(entry.getKey().toString() + " " + entry.getValue());
      }
    }	
  }
  
  public static class PartitioningMapper extends Mapper<LongWritable, Text, ClusterJoinKey, StringElem> {
    TreeMap<StringElem, Integer> pivPts = new TreeMap();
    double eps;
    int numberOfSamples;
    int maxPartitionSize;
    GroupAssignment ga;
    double samplingProbability;
    double inputCardinality;
  
    @Override
    public void setup(Context context) throws FileNotFoundException, IOException {
      // lies die Pivot-Elemente ein
      File f = new File("./pivots");
      String[] pivotfiles = f.list();

      String line;
      for (String pivotfile : pivotfiles) {
        if (pivotfile.contains("crc")) {
          continue;
        }
        BufferedReader lineReader = new BufferedReader(new FileReader("./pivots/" + pivotfile));
        while ((line = lineReader.readLine()) != null) {
          String[] splittedLine = line.split("\\s+");
          if (splittedLine.length > 2) {//3) {
            pivPts.put(new StringElem(splittedLine[1], splittedLine[2]), Integer.parseInt(splittedLine[3]));
          } // else: read wrong file (crc and stuff) -> ignore
        }
      }
      // lies den Schwellwert:
      eps = context.getConfiguration().getDouble("eps", 0);
      numberOfSamples = context.getConfiguration().getInt("numberOfSamples", 0);
      maxPartitionSize = context.getConfiguration().getInt("maxPartitionSize", numberOfSamples / 2);
      ga = new GroupAssignment(1 - eps);
      samplingProbability = context.getConfiguration().getDouble("samplingProbability", 0.0);
      inputCardinality = context.getConfiguration().getDouble("inputCardinality", 0.0);
    }

    private void writeRecordToGroups(ClusterJoinKey outKey, StringElem currentElement, int baseGroup, Context context) throws IOException, InterruptedException {
      outKey.setGroupId(baseGroup);
      context.write(outKey, currentElement);
      if (baseGroup > 1) {
        outKey.setGroupId(baseGroup - 1);
        context.write(outKey, currentElement);
      }
    }
    
     private void writeRecord(ClusterJoinKey outKey, StringElem currentElement, int baseGroup, 
             Context context, int row, int col, HashMap<Integer, Double> pivotDistances, int basePivot, 
             double distanceToBasePivot) throws IOException, InterruptedException {
      outKey.setCol(col);
      outKey.setRow(row);
      outKey.setPartitionID(basePivot);
      outKey.setIsInWindow(false);
      // base partition:
      writeRecordToGroups(outKey, currentElement, baseGroup, context);
      
      // compute window partition assignment:
      for (Map.Entry<Integer, Double> entry : pivotDistances.entrySet()) {
        int piv = entry.getKey();
        if (piv == basePivot) {
          continue;
        }
        double half_distance = (entry.getValue() - distanceToBasePivot) / 2;
        if (half_distance < eps) {
          outKey.setPartitionID(piv);
          outKey.setIsInWindow(true);
          writeRecordToGroups(outKey, currentElement, baseGroup, context); // window partition, group g_i
        }
      }
    }
    
    @Override
    public void map(LongWritable inputKey, Text inputValue, Context context) throws IOException, InterruptedException {
      StringElem currentElement = new StringElem(inputValue.toString());
      if (currentElement.tokens.length == 0) {
        return; // shitty input
      }
      int currentRid = currentElement.getKey();
      

      Integer basePivot = null;
      double estimatedSize = 0;
      double distanceToBasePivot = Double.MAX_VALUE;
      HashMap<Integer, Double> pivotDistances = new HashMap();
      
      for(Map.Entry<StringElem,Integer> entry : pivPts.entrySet()) {
        StringElem piv = entry.getKey();
        double tempDistance = currentElement.getDistanceBetween(piv);
        pivotDistances.put(piv.getKey(), tempDistance);
        if (tempDistance < distanceToBasePivot) {
          distanceToBasePivot = tempDistance;
          basePivot = piv.getKey();
          estimatedSize = entry.getValue();
        }
      }
      
      if (basePivot == null) {
        return; // This happens when a record only consists of a rID and does not contain any token.
      }
      
      ClusterJoinKey outKey = new ClusterJoinKey(basePivot);
      // obtain partition size estimation:
      estimatedSize = estimatedSize / samplingProbability; // this gives us the absolute size of the estimation regarding the real dataset size
      int modulo = (int)Math.ceil((2 * estimatedSize) / inputCardinality); // This is the k as defined in the paper
      ga.setK(modulo); // we assume that the k is equal to the k mentioned in the bucketization description in the paper; at least there is nothing else stated about the k there.
      int baseGroup = ga.getPartitionNumber(currentElement.tokens.length);
      
      if (estimatedSize > maxPartitionSize) {
//        System.out.println("MAP: estimatedSize=" + estimatedSize + ", samplingProbability=" + samplingProbability + ", modulo=" + modulo + ", inputCardinality=" + inputCardinality);
        modulo += 1;
        
        // die Anzahl der Reducer haben wir hier leider nicht... die wäre aber wichtig für die Replikation. den Rest könnte man im Partitionierer machen
//        int currentModulo = currentRid % modulo;
        int currentModulo = Math.abs(inputValue.toString().hashCode()) % modulo; // Der Hash kann auch negativ werden, was für die Berechnung nicht funktioniert
        
        // output rows:
        int row = 0;
        int col = currentModulo;
        for (; row <= currentModulo; row++) {
          writeRecord(outKey, currentElement, baseGroup, context, row, col, pivotDistances, basePivot, distanceToBasePivot); // base and window partitions with enhanced partitioning
        }
        
        // output cols:
        row = currentModulo;
        col++; // otherwise we replicate too much
        for (; col <= modulo - 1; col++) {
          writeRecord(outKey, currentElement, baseGroup, context, row, col, pivotDistances, basePivot, distanceToBasePivot); // base and window partitions with enhanced partitioning
        }
        
      } else {
        writeRecord(outKey, currentElement, baseGroup, context, -1, -1, pivotDistances, basePivot, distanceToBasePivot); // base and window partitions
      }

    }
  }

  public static class VerificationReducer extends Reducer<ClusterJoinKey, StringElem, Text, NullWritable> {
    Text outKey = new Text();
    NullWritable outValue = NullWritable.get();
    double eps;
  
    @Override
    public void setup(Context context) {
      eps = context.getConfiguration().getDouble("eps", 0);
    }
    
    @Override
    public void reduce(ClusterJoinKey key, Iterable<StringElem> values, Context context) throws IOException, InterruptedException {
      boolean doCount = false; // for statistics
      // we get the base records first, then the window records. 
      // we collect and buffer the base records and join them with each other. 
      // subsequently, we join each window record with the base records.
      
      if (!doCount) { // do the join:
        ArrayList<StringElem> readRecords = new ArrayList();
        Iterator<StringElem> it = values.iterator();
        StringElem currentElement;
        while (it.hasNext()) {
          currentElement = it.next();
          for (StringElem compareElement : readRecords) {
            if (compareElement.getKey() != currentElement.getKey() && compareElement.getDistanceBetween(currentElement) <= eps) {
              outKey.set(compareElement.getKey() + " " + currentElement.getKey());
              context.write(outKey, outValue);
            }
          }
          if (!key.getIsInWindow()) {
            readRecords.add(new StringElem(currentElement));
          }
        }
      } else { // only get statistics:
        int countBaseRecords = 0;
        int countWindowRecords = 0;
        Iterator<StringElem> it = values.iterator();
        while (it.hasNext()) {
          it.next();
          if (!key.getIsInWindow()) {
            countWindowRecords++;
          } else {
            countBaseRecords++;
          }
        }
        outKey.set("Reducer-ID: " + context.getTaskAttemptID().getTaskID().getId() + ", " +  key.toString() + ": countBaseRecords=" + countBaseRecords + ", countWindowRecords=" + countWindowRecords);
        context.write(outKey, outValue);
      }
    }	
  }
  
  // in contrast to MRSimJoin, we send the outer/ window partitions to the corresponding base partitions. 
  public static class ClusterJoinPartitioner extends Partitioner<ClusterJoinKey, StringElem> {

    @Override
    public int getPartition(ClusterJoinKey key, StringElem value, int numP) { // this will only be called if the numReduceTasks is set in the Job
      // for the partitioning, we take into account:
      // 1. partitionID (window or not does not matter)
      // 2. groupID
      // 3. row
      // 4. col
//      System.out.println(key.toString());
      
      // Bernstein hash: 
      int h = 0;
      h = key.getPartitionID();
      h = 33 * h + key.getGroupId();
      h = 33 * h + key.getCol();
      h = 33 * h + key.getRow();
      if (h < 0) {
        h = -h;
      }
      return h % numP;
      
      // Davor hatte ich eine kommutative Funktion (Addition) verwendet, was ganz doof ist, weil z. B. h(abc)=h(bca) ist. Wir wollen aber die Reihenfolge möglichst 
      // gut im Hash abgebildet haben, daher habe ich eine verbesserte Version (Bernstein) oben verwendet. 
//      return (key.getPartitionID() + key.getGroupId() + key.getCol() + key.getRow()) % numP; // schon hier geht mir was verloren, wenn ich row und col mit reinnehme!!!  
    }
  }
  
  public static class ClusterJoinSortComparator implements RawComparator<ClusterJoinKey> {

    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
      // we compare in the following order: PID, GID, row, col, isWindow
      
      int pid1 = WritableComparator.readInt(b1, s1);
      int pid2 = WritableComparator.readInt(b2, s2);
      if (pid1 < pid2) {
        return -1;
      } else if (pid1 > pid2) {
        return 1;
      }
      
      s1 += Integer.SIZE / 8;
      s2 += Integer.SIZE / 8;
      int gid1 = WritableComparator.readInt(b1, s1);
      int gid2 = WritableComparator.readInt(b2, s2);
      if (gid1 < gid2) {
        return -1;
      } else if (gid1 > gid2) {
        return 1;
      }
      
      s1 += Integer.SIZE / 8;
      s2 += Integer.SIZE / 8;
      int row1 = WritableComparator.readInt(b1, s1);
      int row2 = WritableComparator.readInt(b2, s2);
      if (row1 < row2) {
        return -1;
      } else if (row1 > row2) {
        return 1;
      }
      
      s1 += Integer.SIZE / 8;
      s2 += Integer.SIZE / 8;
      int col1 = WritableComparator.readInt(b1, s1);
      int col2 = WritableComparator.readInt(b2, s2);
      if (col1 < col2) {
        return -1;
      } else if (col1 > col2) {
        return 1;
      }
      
      s1 += Integer.SIZE / 8;
      s2 += Integer.SIZE / 8;
      boolean isWindow1 = b1[s1] == (byte)1;
      boolean isWindow2 = b2[s2] == (byte)1;
      if (isWindow1 && !isWindow2) { // we sort it so that base records arrive first at the Reducer
        return 1;
      } else if (!isWindow1 && isWindow2) {
        return -1;
      }
      
      return 0;
    }

    @Override
    public int compare(ClusterJoinKey o1, ClusterJoinKey o2) {
      throw new UnsupportedOperationException("Not supported."); //To change body of generated methods, choose Tools | Templates.
    }
  
  }
  
  public static class ClusterJoinGroupComparator implements RawComparator<ClusterJoinKey> {

    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
      // we compare in the following order: PID, GID, row, col
      
      int pid1 = WritableComparator.readInt(b1, s1);
      int pid2 = WritableComparator.readInt(b2, s2);
      if (pid1 < pid2) {
        return -1;
      } else if (pid1 > pid2) {
        return 1;
      }
      
      s1 += Integer.SIZE / 8;
      s2 += Integer.SIZE / 8;
      int gid1 = WritableComparator.readInt(b1, s1);
      int gid2 = WritableComparator.readInt(b2, s2);
      if (gid1 < gid2) {
        return -1;
      } else if (gid1 > gid2) {
        return 1;
      }
      
      s1 += Integer.SIZE / 8;
      s2 += Integer.SIZE / 8;
      int row1 = WritableComparator.readInt(b1, s1);
      int row2 = WritableComparator.readInt(b2, s2);
      if (row1 < row2) {
        return -1;
      } else if (row1 > row2) {
        return 1;
      }
      
      s1 += Integer.SIZE / 8;
      s2 += Integer.SIZE / 8;
      int col1 = WritableComparator.readInt(b1, s1);
      int col2 = WritableComparator.readInt(b2, s2);
      if (col1 < col2) {
        return -1;
      } else if (col1 > col2) {
        return 1;
      }
      
      return 0;
    }

    @Override
    public int compare(ClusterJoinKey o1, ClusterJoinKey o2) {
      throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
  
  }
 
  private static class GroupAssignment {
    double sim;
    double k;
    ArrayList<Integer> partitionAssignment; // id=length => value=partition
    int lastB;
    
    GroupAssignment(double sim) {
      this.sim = sim;
      this.lastB = 0;
      partitionAssignment = new ArrayList();
    }
    
    public void setK(int k) {
      this.k = k;
    }
    
    /**
     * This computes the i of the corresponding bucket. The i is equal to the g_i of the base group.
     * The record of this length has to be distributed to g_i AND g_(i-1).
     * @param length
     * @return 
     */
    public int getPartitionNumber(int length) {
      while (partitionAssignment.size() <= length) {
        lastB++;
        // compute upper bound for this b_i:
        //ABRUNDEN(k/(theta^(i-1))) - 1
        int upperBound = (int)Math.floor(k/Math.pow(sim,(double)lastB - 1.)) - 1;
        int lastLength = 0;
        if (partitionAssignment.size() > 0) {
          lastLength = partitionAssignment.size();
        }
        for (int i = lastLength; i <= upperBound; i++) {
          partitionAssignment.add(lastB);
        }
      }
      return partitionAssignment.get(length);
    }
    
  }

//  public static void main(String[] args) {
//    GroupAssignment ga = new GroupAssignment(.9, 9);
//    for (int l = 1; l < 20; l++) {
//      System.out.println("Länge: " + l + ". Partition: " + ga.getPartitionNumber(l));
//    }
//  }
  
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new Configuration(), new ClusterJoinJob(), args);
  }

  // ****** ALGORITHM NEEDS WHITESPACE-DIVIDED INPUT!!! NO TAB! ***********
  @Override
  public int run(String[] args) throws Exception {
    System.out.println("ClusterJoin");
    ParameterParser pp = new ParameterParser(args);
    double epsDistance = 1 - Double.parseDouble(pp.getTheta());
    String inputPathString = pp.getInput();
    String outputPathString = pp.getOutput() + VernicaJoinDriver.RID_PAIR_PATH; // with duplicates for deduplication

    String pivDir = outputPathString + "piv/PIVOTS/";

    Configuration conf = new Configuration();
    conf.setDouble("eps", epsDistance); // -t (we enter similarity instead of distance)
    int numberOfPivots = pp.getNumberOfPivots();
    if (numberOfPivots == 0) {
      System.out.println("numberOfPivots missing. Setting it to 3.");
      numberOfPivots = 3;
    }
    conf.setInt("numberOfPivots", numberOfPivots); // -n
    int numberOfSamples = pp.getNumberOfSamples(); // -s
//    int numberOfSamples = numberOfPivots; // <<<<<<<<<< in the paper, they don't distinguish these anymore in 8.2.3, so we assume they set them equal.
    conf.setInt("numberOfSamples", numberOfSamples);
    int maxPartitionSize = pp.getNumPartitions(); // this is directly connected to the numberOfSamples! It must be smaller that this number!
    if (maxPartitionSize == 0) {
      throw new Exception("maxPartitionSize (-p) should be larger than 0!");
    }
    conf.setInt("maxPartitionSize", maxPartitionSize); // -p
    
    FileSystem fs = FileSystem.get(conf);
    double inputCardinality = getInputCardinality(pp.getOutput(), fs);
    conf.setDouble("inputCardinality", inputCardinality);
    double samplingProbability = (double)numberOfSamples / inputCardinality;
    conf.setDouble("samplingProbability", samplingProbability);
    
    System.out.println("Parameter: numberOfPivots=" + numberOfPivots  + ", numberOfSamples=" + numberOfSamples + ", maxPartitionSize=" + maxPartitionSize + ", inputCardinality=" + inputCardinality + ", samplingProbability=" + samplingProbability); //   + ", numberOfSamples=" + numberOfSamples + ", modulo=" + modulo

    Path inputPath = new Path(inputPathString);
    Path pivPath = new Path(pivDir + "pivots.txt"); // includes samples as well
    
    Path outputPath = new Path(outputPathString + "1");//(String)nonOptArgs.get(1) + "-" + threshold + "-s" + numStripes);
    fs.delete(outputPath, true); // <<<<<<<<<<<<<<<<< we delete the output path for convenience
    fs.delete(new Path(outputPathString + "piv"), true); // <<<<<<<<<<<<<<<<< we delete the output path for convenience
    fs.delete(new Path(outputPathString), true); // <<<<<<<<<<<<<<<<< we delete the output path for convenience
    
    writeSample(fs, inputPath, pivPath, pp.getNumberOfPivots() + pp.getNumberOfSamples());
    
    
    
    
    
    Job job1 = Job.getInstance(conf, "ClusterJoin Partition Cardinality Estimation Step");
    job1.setJarByClass(ClusterJoinJob.class);

    FileInputFormat.addInputPath(job1, pivPath);
    FileOutputFormat.setOutputPath(job1, outputPath);
    job1.setMapperClass(CardinalityEstimationMapper.class);
    job1.setMapOutputKeyClass(IntWritable.class);
    job1.setMapOutputValueClass(Text.class);
    job1.setOutputKeyClass(LongWritable.class);
    job1.setOutputValueClass(Text.class);
    job1.setReducerClass(CardinalityEstimationReducer.class);
    job1.waitForCompletion(true);
    
    
    
    Job job2 = Job.getInstance(conf, "ClusterJoin Partitioning, Replication, and Verification Step");
    job2.setJarByClass(ClusterJoinJob.class);
    job2.addCacheFile(new URI(outputPath.toString() + "#pivots"));
    
    job2.setMapperClass(PartitioningMapper.class);
    job2.setMapOutputKeyClass(ClusterJoinKey.class);
    job2.setMapOutputValueClass(StringElem.class);
    
    job2.setPartitionerClass(ClusterJoinPartitioner.class); // groups base partitions and window partitions together and hashes them to the number of reducers
    job2.setSortComparatorClass(ClusterJoinSortComparator.class);
    job2.setGroupingComparatorClass(ClusterJoinGroupComparator.class);
        
    if (job2.getNumReduceTasks() == 1) {
      job2.setNumReduceTasks(4); // das muss zwingend gesetzt sein, sonst wird der Partitioner nicht gerufen und es gibt falsche Ergebnisse
    }
    job2.setReducerClass(VerificationReducer.class);
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(NullWritable.class);
    FileInputFormat.addInputPath(job2, inputPath);
    FileOutputFormat.setOutputPath(job2, new Path(outputPathString));
    
    job2.waitForCompletion(true);
    fs.delete(outputPath, true);
    fs.delete(new Path(outputPathString + "piv"), true);
    
    return 0;
  }
  
  public int getInputCardinality(String inputPathString, FileSystem fs) throws IOException {
      Path pt = new Path(inputPathString + ClusterJoinDriver.CARDINALITY_SUFFIX + "/part-r-00000");
      BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
      String line = br.readLine();
      String[] tmp = line.split("\\s+"); // "1 FREQUENCY"
      br.close();
      return Integer.parseInt(tmp[1]);
  }
  
  public void writeSample(FileSystem fs, Path inputPath, Path pivPath, int numberOfSamples) throws IOException {
    Object[] pivots = getPivots(inputPath, numberOfSamples); //generate pivot points from inputPath directory
    FSDataOutputStream out = fs.create(pivPath); //write pivots to file      
    for (Object pivot : pivots) {
      out.writeBytes(((Text) pivot).toString() + '\n');
    }
    out.close();
  }

  Object[] getPivots(Path input, int numPivs) throws IOException {
    JobConf job = new JobConf();
    job.setInputFormat(KeyValueTextInputFormat.class); // <<<<< akzeptiert nur Leerzeichen, keine Tabs
    job.setMapOutputKeyClass(Text.class);
    org.apache.hadoop.mapred.FileInputFormat.addInputPath(job, input);
    final KeyValueTextInputFormat inf = (KeyValueTextInputFormat) job.getInputFormat();
    InputSampler.Sampler<Text, Text> sampler = new InputSampler.RandomSampler(1.0, numPivs, 100);
    Object[] samples = sampler.getSample(inf, job);
    return samples;
  }

}
