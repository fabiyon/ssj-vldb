package de.huberlin.textualsimilarityhadoop;

import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;
import org.apache.commons.io.FileUtils;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
//import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
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
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

public class MRSimJoinJob extends Configured implements Tool {
  private static final Logger log = Logger.getLogger(MRSimJoinJob.class);
  private static final int numFilesToGen = 1;
  private static final double errorAdjsmt = 0.000000000000005;
  private static final int constSmallNum = 100;

  

  public static class CloudJoinKey implements WritableComparable<CloudJoinKey> {

    long partitionID = -1;
    long windowID = -1;
    long prevItrPartion = -1;

    CloudJoinKey() {
    }

    CloudJoinKey(long partitionID) {
      this.partitionID = partitionID;
    }

    CloudJoinKey(long partitionID, long windowID) {
      this.partitionID = partitionID;
      this.windowID = windowID;
    }

    CloudJoinKey(long partitionID, long windowID, long prevItrPartion) {
      this.partitionID = partitionID;
      this.windowID = windowID;
      this.prevItrPartion = prevItrPartion;
    }

    public long getPartitionID() {
      return partitionID;
    }

    public void setPartitionID(long partitionID) {
      this.partitionID = partitionID;
    }

    public long getWindowID() {
      return windowID;
    }

    public void setWindowID(long windowID) {
      this.windowID = windowID;
    }

    public long getPrevItrPartion() {
      return prevItrPartion;
    }

    public void setPrevItrPartion(long prevItrPartion) {
      this.prevItrPartion = prevItrPartion;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      partitionID = in.readLong();
      windowID = in.readLong();
      prevItrPartion = in.readLong();
    }

    @Override
    public void write(DataOutput out) throws IOException {
      out.writeLong(partitionID);
      out.writeLong(windowID);
      out.writeLong(prevItrPartion);
    }

    @Override
    public int compareTo(CloudJoinKey o) {
      if (this.partitionID != o.getPartitionID()) {
        return this.partitionID < o.getPartitionID() ? -1 : 1;
      } else if (this.windowID != o.getWindowID()) {
        return this.windowID < o.getWindowID() ? -1 : 1;
      } else if (this.prevItrPartion != o.getPrevItrPartion()) {
        return this.prevItrPartion < o.getPrevItrPartion() ? -1 : 1;
      } else {
        return 0;
      }
    }

    @Override
    public String toString() {
      return "PID: " + partitionID + " WID: " + windowID + " PIP: " + prevItrPartion;
    }
  }
  
  public static ArrayList<StringElem> getPivotElements(Mapper.Context context) {
    System.out.println("============getPivotElements wurde gerufen=============");
    try {
      URI[] uris = context.getCacheFiles();
      if (uris != null || uris.length > 0 && uris[0] != null) {
        BufferedReader lineReader = new BufferedReader(new FileReader("./pivots")); // this corresponds to the name added to the URI after the # sign. Hadoop copies a symlink of it into the current working path
        ArrayList<StringElem> pivPts = new ArrayList();
        String line;
        while ((line = lineReader.readLine()) != null) {
          pivPts.add(new StringElem(line));
        }
        System.out.println("========= pivPts.length:" + pivPts.size());
        return pivPts;
      } else {
        System.out.println("=========== BM Error accessing distributed cache");
        log.debug("BM Error accessing distributed cache");
      }
    } catch (IOException ioe) {
      System.out.println("========== BM IOException reading from distributed cache");
      log.debug("BM IOException reading from distributed cache");
      log.debug(ioe.toString());
    }
    return null;
  }
  
  // this function is used for both the base and the window mapper:
  public static void genericMap(LongWritable inputKey, Text inputValue, Mapper.Context context, 
          ArrayList<StringElem> pivPts, double eps, int mapType) throws IOException, InterruptedException {
    String mapTypeString;
    if (mapType == 0) {
      mapTypeString = "Base";
    } else {
      mapTypeString = "Window";
    }
    log.debug("===================================" + mapTypeString + " Map Begin===================================");
      
    StringElem elem;
    try {
      elem = new StringElem(inputValue.toString()); //creates element that is being worked on in this map
    } catch (Exception e) {
      log.error(inputValue.toString(), e);
      return;
    }

    StringElem pivot_curr; //tracks the current pivot point
    StringElem pivot_best; //current best pivot point/partition
    double min_distance; //current min distance between elem and pivot_best

//    System.out.println("======= genericMap wurde gerufen =========");
//    System.out.println("======= eps: " + eps + " =========");
//    
//    if (pivPts == null) {
//      System.out.println("======= pivPts war null!!! =========");
//      pivPts = getPivotElements(context);
//    }
//    if (pivPts == null) {
//      System.out.println("======= pivPts ist immer noch null!!! FCK =========");
//    }
    
    Iterator<StringElem> pivItr = pivPts.iterator();
    if (pivItr.hasNext()) {
      pivot_best = pivItr.next();
      min_distance = elem.getJaccardDistanceBetween(pivot_best);
    } else {
      log.error(mapTypeString + "Map Error: No pivot elements");
      return;
    }
    while (pivItr.hasNext()) { //check all partitions point and find best match
      pivot_curr = pivItr.next();
      double curr_distance = elem.getJaccardDistanceBetween(pivot_curr);
      if (curr_distance < min_distance) { //if the current partition is closer than the current best
        pivot_best = pivot_curr; //update pivot_best
        min_distance = curr_distance; //update current distance
      }
    }
    elem.setPartitionID(pivot_best.getKey()); //set element native partition (may not be needed)
    CloudJoinKey cjk;
    
    if (mapType == 0) { // base:
      cjk = new CloudJoinKey(pivot_best.getKey()); //creates a key for writing to the context
    } else { // window:
      cjk = new CloudJoinKey(pivot_best.getKey(), -1, elem.getPrevPartition()); // in the middle, the windowID is set with -1, because it will be set subsequently
    }
    log.debug("Context.write(" + cjk.toString() + ", element key: " + elem.getKey() + ")");
    context.write(cjk, elem);

    pivItr = pivPts.iterator(); //check if the elem is in a window of any of the other partitions
    while (pivItr.hasNext()) {
      pivot_curr = pivItr.next(); //get a current working pivot
      if (pivot_curr.getKey() == pivot_best.getKey()) {
        continue; //if the current pivot is the same as the best match, continue onto next pivot
      }
      double curr_distance = elem.getJaccardDistanceBetween(pivot_curr);//get the distance between the current pivot and the elem

      if (((curr_distance - min_distance) / 2) <= eps) { //check if it's in in the range of the window
        //add to window partition
        cjk.setWindowID(pivot_curr.getKey()); //set the window id on the keyto the key key of the current pivot
        log.debug("Context.write(" + cjk.toString() + ", element key: " + elem.getKey() + ")");
        context.write(cjk, elem);
      }
    }
    log.debug("===================================" + mapTypeString + " Map End===================================");
  }

  public static class CloudJoinBaseMapper extends Mapper<LongWritable, Text, CloudJoinKey, StringElem> {
    ArrayList<StringElem> pivPts;
    double eps;

    @Override
    protected void setup(Context context) {
      pivPts = getPivotElements(context);
      String s = context.getConfiguration().get("eps", "");
      if ("".equals(s)) {
        log.error("BM Error: Unable to get eps from configuration");
      }
      eps = Double.parseDouble(s);
    }

    @Override
    public void map(LongWritable inputKey, Text inputValue, Context context) throws IOException, InterruptedException {
      genericMap(inputKey, inputValue, context, pivPts, eps, 0);
    } //end map
  } //end CloudJoinBaseMapper

  public static class CloudJoinWindowMapper extends Mapper<LongWritable, Text, CloudJoinKey, StringElem> {
    ArrayList<StringElem> pivPts;
    double eps;

    @Override
    protected void setup(Context context) {
      pivPts = getPivotElements(context);
      String s = context.getConfiguration().get("eps", "");
      if ("".equals(s)) {
        log.error("BM Error: Unable to get eps from configuration");
      }
      eps = Double.parseDouble(s);
    }

    @Override
    public void map(LongWritable inputKey, Text inputValue, Context context) throws IOException, InterruptedException {
      genericMap(inputKey, inputValue, context, pivPts, eps, 1);
    }//end map
  }//end CloudJoinWindowMapper

  public static class CloudJoinBasePartitioner extends Partitioner<CloudJoinKey, StringElem> {

    @Override
    public int getPartition(CloudJoinKey key, StringElem value, int numP) {
      if (key.getWindowID() != (-1)) {//if it's a window partition, send to window partition
        long min = Math.min(key.getPartitionID(), key.getWindowID());
        long max = Math.max(key.getPartitionID(), key.getWindowID());
        log.debug("PP Key PID: " + key.getPartitionID() + " Partition: " + (int) ((min * 157 + max * 127) % numP));
        return (int) ((min * 157 + max * 127) % numP);
      } else {//if it's it's native partition, send to it's native partition

        log.debug("PW Key PID: " + key.getPartitionID() + " Partition: " + (int) ((key.getPartitionID() * 149) % numP));
        return (int) ((key.getPartitionID() * 149) % numP);
      }
    }

  }

  public static class CloudJoinWindowPartitioner extends Partitioner<CloudJoinKey, StringElem> implements Configurable {

    private Configuration conf;

    @Override
    public int getPartition(CloudJoinKey key, StringElem value, int numP) {
      //we should perform a check here and make sure we get values back
      long W = Long.parseLong(conf.get("W"));
      long V = Long.parseLong(conf.get("V"));

      if (key.getWindowID() == -1) {
        return (int) ((key.getPartitionID() * 163) % numP);
      } else {
        long min = Math.min(key.getPartitionID(), key.getWindowID());
        long max = Math.max(key.getPartitionID(), key.getWindowID());

        if (((key.getPartitionID() < key.getWindowID()) && (key.getPrevItrPartion() == W))
                || ((key.getPartitionID() > key.getWindowID()) && (key.getPrevItrPartion() == V))) {
          return (int) ((min * 179 + max * 197) % numP);
        } else {
          return (int) ((min * 173 + max * 181) % numP);
        }
      }
    }

    @Override
    public Configuration getConf() {
      return conf;
    }

    @Override
    public void setConf(Configuration conf) {
      this.conf = conf;
    }

  }

  public static class CloudJoinBaseGrouper implements RawComparator<CloudJoinKey> {

    @Override
    public int compare(CloudJoinKey key1, CloudJoinKey key2) {

      long part1 = key1.getPartitionID();
      long win1 = key1.getWindowID();
      long part2 = key2.getPartitionID();
      long win2 = key2.getWindowID();
      if (win1 == -1 && win2 == -1) {//p-p
        if (part1 == part2) {
          return 0;
        } else {
          return (part1 < part2 ? -1 : 1);
        }
      } else if (win1 == -1) { //p-w
        return -1;
      } else if (win2 == -1) { //w-p
        return 1;
      } else {//w-w
        long minOfo1 = Math.min(part1, win1);
        long maxOfo1 = Math.max(part1, win1);

        long minOfo2 = Math.min(part2, win2);
        long maxOfo2 = Math.max(part2, win2);

        if (minOfo1 == minOfo2) {
          if (maxOfo1 == maxOfo2) {
            return 0;
          } else {
            return (maxOfo1 < maxOfo2 ? -1 : 1);
          }
        } else {
          return (minOfo1 < minOfo2 ? -1 : 1);
        }
      }
    }

    @Override
    public int compare(byte[] b1, int s1, int l1,
            byte[] b2, int s2, int l2) {

      long part1 = WritableComparator.readLong(b1, s1);
      long win1 = WritableComparator.readLong(b1, s1 + (Long.SIZE / 8));
      long part2 = WritableComparator.readLong(b2, s2);
      long win2 = WritableComparator.readLong(b2, s2 + (Long.SIZE / 8));

      if (win1 == -1 && win2 == -1) {//p-p
        if (part1 == part2) {
          return 0;
        } else {
          return (part1 < part2 ? -1 : 1);
        }
      } else if (win1 == -1) { //p-w
        return -1;
      } else if (win2 == -1) { //w-p
        return 1;
      } else {//w-w
        long minOfo1 = Math.min(part1, win1);
        long maxOfo1 = Math.max(part1, win1);

        long minOfo2 = Math.min(part2, win2);
        long maxOfo2 = Math.max(part2, win2);

        if (minOfo1 == minOfo2) {
          if (maxOfo1 == maxOfo2) {
            return 0;
          } else {
            return (maxOfo1 < maxOfo2 ? -1 : 1);
          }
        } else {
          return (minOfo1 < minOfo2 ? -1 : 1);
        }
      }
    }

  }

  public static class CloudJoinWindowGrouper implements RawComparator<CloudJoinKey>, Configurable {

    private Configuration conf;

    @Override
    public int compare(CloudJoinKey key1, CloudJoinKey key2) {
      long part1 = key1.getPartitionID();
      long win1 = key1.getWindowID();
      long part2 = key2.getPartitionID();
      long win2 = key2.getWindowID();

      if ((win1 == -1) && (win2 == -1))//p-p
      {
        if (part1 == part2) {
          return 0;
        } else {
          return (part1 < part2 ? -1 : 1);
        }
      } else if ((win1 == -1) && (win2 != -1)) //p-w
      {
        return -1;
      } else if ((win1 != -1) && (win2 == -1)) //w-p
      {
        return 1;
      } else {

        long min1 = Math.min(part1, win1);
        long max1 = Math.max(part1, win1);
        long min2 = Math.min(part2, win2);
        long max2 = Math.max(part2, win2);

        if (!((min1 == min2) && (max1 == max2))) {
          if (min1 == min2) {
            return (max1 < max2 ? -1 : 1);
          } else {
            return (min1 < min2 ? -1 : 1);
          }
        } else {
          long prevPart1 = key1.getPrevItrPartion();
          long prevPart2 = key2.getPrevItrPartion();
          //we should perform a check here and make sure we get values back
          long W = Long.parseLong(conf.get("W"));
          long V = Long.parseLong(conf.get("V"));

          if ((part1 == part2) && (prevPart1 == prevPart2)) {
            return 0;
          } else if (part1 == part2) {
            if (part1 < win1) {
              if ((prevPart1 == V) && (prevPart2 == W)) {
                return -1;
              } else {
                return 1;
              }
            } else {
              if ((prevPart1 == V) && (prevPart2 == W)) {
                return 1;
              } else {
                return -1;
              }
            }
          } else if (prevPart1 == prevPart2) {
            if (prevPart1 == V) {
              if (part1 < win1) {
                return -1;
              } else {
                return 1;
              }
            } else {
              if (part1 < win1) {
                return 1;
              } else {
                return -1;
              }
            }
          } else {
            return 0;
          }
        }
      }

    }// end compare

    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {

      //this is going to be needing some optimization but this is the easiest way to code this right now.
      long part1 = WritableComparator.readLong(b1, s1);
      long win1 = WritableComparator.readLong(b1, s1 + (Long.SIZE / 8));
      long part2 = WritableComparator.readLong(b2, s2);
      long win2 = WritableComparator.readLong(b2, s2 + (Long.SIZE / 8));

      if ((win1 == -1) && (win2 == -1))//p-p (partition - partition)
      {
        if (part1 == part2) {
          return 0;
        } else {
          return (part1 < part2 ? -1 : 1);
        }
      } else if ((win1 == -1) && (win2 != -1)) //p-w (partition - window)
      {
        return -1; // defines an order over the "secondary key" window ID
      } else if ((win1 != -1) && (win2 == -1)) //w-p
      {
        return 1;
      } else {

        long min1 = Math.min(part1, win1);
        long max1 = Math.max(part1, win1);
        long min2 = Math.min(part2, win2);
        long max2 = Math.max(part2, win2);

        if (!((min1 == min2) && (max1 == max2))) {
          if (min1 == min2) {
            return (max1 < max2 ? -1 : 1);
          } else {
            return (min1 < min2 ? -1 : 1);
          }
        } else {
          long prevPart1 = WritableComparator.readLong(b1, s1 + (Long.SIZE + Long.SIZE) / 8);
          long prevPart2 = WritableComparator.readLong(b2, s2 + (Long.SIZE + Long.SIZE) / 8);

          //we should perform a check here and make sure we get values back
          long W = Long.parseLong(conf.get("W"));
          long V = Long.parseLong(conf.get("V"));

          if ((part1 == part2) && (prevPart1 == prevPart2)) {
            return 0;
          } else if (part1 == part2) {
            if (part1 < win1) {
              if ((prevPart1 == V) && (prevPart2 == W)) {
                return -1;
              } else {
                return 1;
              }
            } else {
              if ((prevPart1 == V) && (prevPart2 == W)) {
                return 1;
              } else {
                return -1;
              }
            }
          } else if (prevPart1 == prevPart2) {
            if (prevPart1 == V) {
              if (part1 < win1) {
                return -1;
              } else {
                return 1;
              }
            } else {
              if (part1 < win1) {
                return 1;
              } else {
                return -1;
              }
            }
          } else {
            return 0;
          }
        }
      }
    }

    @Override
    public Configuration getConf() {
      return conf;
    }

    @Override
    public void setConf(Configuration conf) {
      this.conf = conf;
    }

  }

  public static void genericReducer(CloudJoinKey key, Iterable<StringElem> values, 
          Context context, double eps, long memory, long itr, String outDir, int reduceType) {    
    String reduceString;
    if (reduceType == 0) {
      reduceString = "Base";
    } else {
      reduceString = "Window";
    }
    
    log.debug("===================================" + reduceString + " Reduce Begin===================================");
      
    boolean performInMemoryJoin = true;
    ArrayList<StringElem> inMemoryList = new ArrayList<StringElem>();
    ArrayList<StringElem> inMemoryList1 = new ArrayList<StringElem>(); // added by FF. Used in base rounds in window partitions
    long firstWindowId = -1;

    log.debug("Start inmemory collection. CJK: " + key.toString());
    Iterator<StringElem> valItr = values.iterator();
    int counter = 0;
    int counter1 = 0;
    long usedMemory = 0;
    while (valItr.hasNext()) {
      StringElem ptrElem = valItr.next();
      StringElem currElem = new StringElem(ptrElem.toString());
      currElem.setPartitionID(ptrElem.getPartitionID());
      currElem.setWindowID(ptrElem.getWindowID());
      currElem.setPrevPartition(ptrElem.getPrevPartition());

      usedMemory += currElem.getSizeInBytes();
      
      if (key.getWindowID() == -1) {
        inMemoryList.add(counter, currElem);
        counter++;
      } else {
        if (firstWindowId == -1) {
          firstWindowId = key.getWindowID();
        }
        if (firstWindowId == key.getWindowID()) {
          inMemoryList.add(counter, currElem);
          counter++;
        } else {
          inMemoryList1.add(counter1, currElem);
          counter1++;
        }
      }
      
      //if we have reached the limit of memory and there is more to do, break and don't perform inMemoryJoin
      if ((memory <= usedMemory) && valItr.hasNext()) {
        log.debug("We are not performing in memory join: " + memory + " <= " + usedMemory);
        performInMemoryJoin = false;
        break;
      }
    }

    if (performInMemoryJoin) {
      log.debug("Perform InMemoryJoin - Base");
      //for all elements in inMemoryList, 
      //check if they can be joined to all others
      //if they can, write output to final output
      if (key.getWindowID() == -1) {
        baseQuickJoin(inMemoryList, eps, context, key.getWindowID(), reduceType);
      } else {
        baseQuickJoinWin(inMemoryList, inMemoryList1, eps, context, key.getWindowID(), reduceType);
      }
    } else { //perform out of memory operations
      FileSystem fs; //create variable for file system access
      Path outfile;
      Path itrOutDir;
      FSDataOutputStream out;
      counter = 0;

      if (key.getWindowID() == -1) { //it's a base partition
        try {
          fs = FileSystem.get(context.getConfiguration()); //try to set up a output stream to the HDFS

          if (reduceType == 0) {
            itrOutDir = new Path(outDir + "intermediate_output_" + itr + "_" + key.getPartitionID());
          } else {
            itrOutDir = new Path(outDir + "intermediate_output_" + itr + "_" + context.getConfiguration().get("V") + "_" + context.getConfiguration().get("W") + "_" + key.partitionID);
          }
          fs.mkdirs(itrOutDir);
          outfile = new Path(itrOutDir.toString() + "/" + itr + ".txt");
          out = fs.create(outfile);
          log.debug("Output Dir: " + itrOutDir.toString());
        } catch (IOException e) {
          log.debug(reduceString + " Reduce WindowID = -1: Error creating directory/file");
          log.debug(e.getMessage()); //error: unable to create stream
          return;
        }

        // write out all following elements in the input iterator:
        while (valItr.hasNext()) {
          StringElem currElem = valItr.next();
          try {
            if (reduceType == 0) {
              splitAndWrite(currElem.toString() + '\n', out);
//              out.writeBytes(currElem.toString() + '\n');
              log.debug("to_file(key: " + currElem.getKey() + ")");
            } else {
              splitAndWrite(currElem.toStringPrev() + '\n', out);
//              out.writeBytes(currElem.toStringPrev() + '\n');
              log.debug("to_file(key: " + currElem.getKey() + ", PrevPartition: " + currElem.getPrevPartition() +  ")");
            }
            counter++;
          } catch (IOException e) {
            log.debug(reduceString + " Reduce WindowID = -1: Error writing remaining elements in que");
            log.debug(e.getMessage());
          }
        }
        if (reduceType == 1) { // in this case, we also need the previous partition ID
          reduceType = 2;
        }
        writeToStream(inMemoryList, out, reduceString, reduceType);
        writeToStreamAndClose(inMemoryList1, out, reduceString, reduceType); // will never contain an element...
      } else { //it's a window partition
        long min = Math.min(key.getPartitionID(), key.getWindowID());
        long max = Math.max(key.getPartitionID(), key.getWindowID());
        try {
          fs = FileSystem.get(context.getConfiguration());					//try to set up a output stream to the HDFS
          if (reduceType == 0) {
            itrOutDir = new Path(outDir + "intermediate_output_" + itr + "_" + min + "_" + max);
          } else {
            itrOutDir = new Path(outDir + "intermediate_output_" + itr + "_" + key.getPartitionID() + "_" + key.getWindowID() + "_" + key.getPrevItrPartion());
          }
          fs.mkdirs(itrOutDir);
            outfile = new Path(itrOutDir.toString() + "/" + itr + ".txt");
            out = fs.create(outfile);
          log.debug("Output Dir: " + itrOutDir.toString());
        } catch (IOException e) {
          log.debug(reduceString + " Reduce WindowID != -1: Error creating directory/file");
          log.debug(e.getMessage());						//error: unable to create stream
          return;
        }

        while (valItr.hasNext()) {
          StringElem currElem = valItr.next();
          try {
            splitAndWrite(currElem.toStringPart() + '\n', out);
//            out.writeBytes(currElem.toStringPart() + '\n');
            log.debug("to_file(key: " + currElem.getKey() + ", Partition: " + currElem.getPartitionID() +  ")");
            counter++;
          } catch (IOException e) {
            log.debug(reduceString + " Reduce WindowID != -1: Error writing remaining elements in que");
            log.debug(e.getMessage());
          }
        }

        writeToStream(inMemoryList, out, reduceString, 1); // reduceType=1 forces usage of StringElem.toStringPart() instead of toString() to represent the window ID
        writeToStreamAndClose(inMemoryList1, out, reduceString, 1);
      }//end it's a window partition
      log.debug("Intermediate elements commited to HDFS: " + counter);
    }//end out of memory operations
    log.debug("===================================" + reduceString + " Reduce End===================================");
  }
  
  // workaround for bug https://issues.apache.org/jira/browse/HDFS-7765:
          /**
        Error: java.lang.ArrayIndexOutOfBoundsException: 4608
	at org.apache.hadoop.fs.FSOutputSummer.write(FSOutputSummer.java:76)
	at org.apache.hadoop.fs.FSDataOutputStream$PositionCache.write(FSDataOutputStream.java:50)
	at java.io.DataOutputStream.writeBytes(DataOutputStream.java:276)
	at de.huberlin.textualsimilarityhadoop.MRSimJoinJob.writeToStream(MRSimJoinJob.java:749)
	at de.huberlin.textualsimilarityhadoop.MRSimJoinJob.genericReducer(MRSimJoinJob.java:732)
	at de.huberlin.textualsimilarityhadoop.MRSimJoinJob$CloudJoinWindowReducer.reduce(MRSimJoinJob.java:1159)
	at de.huberlin.textualsimilarityhadoop.MRSimJoinJob$CloudJoinWindowReducer.reduce(MRSimJoinJob.java:1128)
	at org.apache.hadoop.mapreduce.Reducer.run(Reducer.java:171)
	at org.apache.hadoop.mapred.ReduceTask.runNewReducer(ReduceTask.java:627)
	at org.apache.hadoop.mapred.ReduceTask.run(ReduceTask.java:389)
	at org.apache.hadoop.mapred.YarnChild$2.run(YarnChild.java:163)
	at java.security.AccessController.doPrivileged(Native Method)
	at javax.security.auth.Subject.doAs(Subject.java:415)
	at org.apache.hadoop.security.UserGroupInformation.doAs(UserGroupInformation.java:1628)
	at org.apache.hadoop.mapred.YarnChild.main(YarnChild.java:158)
           */
  private static void splitAndWrite(String input, FSDataOutputStream out) throws IOException {
    String[] tmpArr = input.split("(?<=\\G.{1150})"); // UFT-8: 4x String characters = bytes
    for (String tmp : tmpArr) {
      System.out.println("Länge: " + tmp.length());
      out.writeBytes(tmp);
    }
  }
  
  private static void writeToStream(ArrayList<StringElem> inMemoryList, FSDataOutputStream out, String reduceString, int reduceType) {
    Iterator<StringElem> inMemoryItr = inMemoryList.iterator();
    while (inMemoryItr.hasNext()) {
      StringElem currElem = inMemoryItr.next();
      try {
        if (reduceType == 0) {
//          out.writeBytes(currElem.toString() + '\n');
          splitAndWrite(currElem.toString() + '\n', out);
          log.debug("to_file(key: " + currElem.getKey() + ")");
        } else if (reduceType == 1) {
          splitAndWrite(currElem.toStringPart() + '\n', out);
//          String outString = currElem.toStringPart() + '\n';
          
//          if (outString.length() < 4608) {
//            out.writeBytes(outString);
//          } else {
//            // split into parts that are smaller than 4608:
//            String[] tmpArr = outString.split("(?<=\\G.{4600})");
//            for (String tmp : tmpArr) {
//              out.writeBytes(tmp);
//            }
//          }
          log.debug("to_file(key: " + currElem.getKey() + ", Partition: " + currElem.getPartitionID() +  ")");
        } else {
          splitAndWrite(currElem.toStringPrev() + '\n', out);
//          out.writeBytes(currElem.toStringPrev() + '\n');
          log.debug("to_file(key: " + currElem.getKey() + ", PrevPartition: " + currElem.getPrevPartition() +  ")");
        }
      } catch (IOException e) {
        log.debug(reduceString + " Reduce WindowID = -1: Error writing inMemoryList elements");
        log.debug(e.getMessage());
      }
    }
  }
  
  private static void writeToStreamAndClose(ArrayList<StringElem> inMemoryList, FSDataOutputStream out, String reduceString, int reduceType) {
    writeToStream(inMemoryList, out, reduceString, reduceType);
    try {
      out.close();
    } catch (IOException e) {
      log.debug(reduceString + " Reduce WindowID != -1: Error closing outfiles");
    }
  }
  
  private static void baseQuickJoin(List<StringElem> objs, double eps, Context context, long code, int reduceType) {
//      log.debug("Begin: baseQuickJoin size: " + objs.size());
    if (objs.size() < constSmallNum || true) { // <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
      baseNestedLoop(objs, eps, context, code, reduceType);
    } else {
      int p1Index;
      int p2Index;

      p1Index = (int) (Math.random() * (objs.size()));
      do {
        p2Index = (int) (Math.random() * (objs.size()));
      } while (p1Index == p2Index);

      //log.debug("P2Index " + objs.get(p2Index).toString() + ": baseQuickJoin");
      //log.debug("Call basePartition: baseQuickJoin");
      basePartition(objs, eps, context, code, objs.get(p1Index), objs.get(p2Index), reduceType);
    }
  } //end BaseQuickJoin
  
  private static void basePartition(List<StringElem> objs, double eps, Context context, long code, StringElem p1, StringElem p2, int reduceType) {
//      log.debug("Begin: basePartition size: " + objs.size());
    int r = 0;
    int startIndx = 0;
    int endIndx = objs.size() - 1;

    ArrayList<StringElem> winP1 = new ArrayList<StringElem>();
    ArrayList<StringElem> winP2 = new ArrayList<StringElem>();

    double startDist = (objs.get(startIndx).getJaccardDistanceBetween(p1) - objs.get(startIndx).getJaccardDistanceBetween(p2)) / 2;
    double endDist = (objs.get(endIndx).getJaccardDistanceBetween(p1) - objs.get(endIndx).getJaccardDistanceBetween(p2)) / 2;
    while (startIndx < endIndx) {

      while (endDist > r && startIndx < endIndx) {//closest to p2
        if (endDist <= (r + (eps + errorAdjsmt))) {
          winP1.add(objs.get(endIndx));
        }
        endIndx--;
        endDist = (objs.get(endIndx).getJaccardDistanceBetween(p1) - objs.get(endIndx).getJaccardDistanceBetween(p2)) / 2;
      }

      while (startDist <= r && startIndx < endIndx) {//closest to p1
        if (startDist >= (r - (eps + errorAdjsmt)) && startIndx < endIndx) {
          winP2.add(objs.get(startIndx));
        }
        startIndx++;
        startDist = (objs.get(startIndx).getJaccardDistanceBetween(p1) - objs.get(startIndx).getJaccardDistanceBetween(p2)) / 2;
      }

      if (startIndx < endIndx) {
        if (endDist >= (r - (eps + errorAdjsmt))) {
          winP2.add(objs.get(endIndx));
        }
        if (startDist <= (r + (eps + errorAdjsmt))) {
          winP1.add(objs.get(startIndx));
        }

        //exchange positions//
        StringElem temp = objs.get(startIndx);
        objs.set(startIndx, objs.get(endIndx));
        objs.set(endIndx, temp);

        startIndx++;
        startDist = (objs.get(startIndx).getJaccardDistanceBetween(p1) - objs.get(startIndx).getJaccardDistanceBetween(p2)) / 2;

        endIndx--;
        endDist = (objs.get(endIndx).getJaccardDistanceBetween(p1) - objs.get(endIndx).getJaccardDistanceBetween(p2)) / 2;
      }

    }//end outer while loop

    if (startIndx == endIndx) {
      if (endDist > r && endDist <= (r + (eps + errorAdjsmt))) {
        winP1.add(objs.get(endIndx));
      }

      if (startDist <= r && startDist >= (r - (eps + errorAdjsmt))) {
        winP2.add(objs.get(startIndx));
      }

      if (endDist > r) {
        endIndx--;
      }
    }

    //log.debug("endIndx = " + endIndx + ": basePartition");
    //log.debug("Call baseQuickJoin: basePartition");
    baseQuickJoin(objs.subList(0, endIndx + 1), eps, context, code, reduceType);
    baseQuickJoin(objs.subList(endIndx + 1, objs.size()), eps, context, code, reduceType);
    //log.debug("trim: basePartition");
    winP1.trimToSize();
    winP2.trimToSize();
    //log.debug("Call baseQuickJoinWin: basePartition");
    baseQuickJoinWin(winP1.subList(0, winP1.size()), winP2.subList(0, winP2.size()), eps, context, code, reduceType);

  }//end basePartition
  
  private static void baseQuickJoinWin(List<StringElem> objs1, List<StringElem> objs2, double eps, Context context, long code, int reduceType) {
    log.debug("Begin: baseQuickJoinWin size1: " + objs1.size() + " size2: " + objs2.size());
    int totalSize = (objs1.size() + objs2.size());
    if (totalSize < constSmallNum || true) { /// <<<<<<<<<<<<<<<<<<<<<<<<<<
      log.debug("Call Nested Loop: baseQuickJoinWin");
      baseNestedLoop(objs1, objs2, eps, context, code, reduceType);
    } else {
      int p1Index;
      int p2Index;

      p1Index = (int) (Math.random() * (totalSize));
      do {
        p2Index = (int) (Math.random() * (totalSize));
      } while (p1Index == p2Index); // <<<<<<<<<<<<<<<<<< das kann unendlich sein. ;-)

      if (!objs1.isEmpty() && !objs2.isEmpty()) {
        // das ist wirr: der 6. Parameter ist Pivot1. Der wird aber im Falle von p1Index >= objs2.size() von objs2 nach einer nicht nachvollziebaren Logik gezogen. Diese kann insbesondere dazu führen, dass der gezogene Index out of bounds ist...
        basePartitionWin(objs1, objs2, eps, context, code, (p1Index < objs1.size() ? (objs1.get(p1Index)) : (objs2.get(p1Index - objs1.size()))), (p2Index < objs1.size() ? (objs1.get(p2Index)) : (objs2.get(p2Index - objs1.size()))), reduceType);
      }
    }

  }//end baseQuickJoinWin	
  
  private static void basePartitionWin(List<StringElem> objs1, List<StringElem> objs2, double eps, Context context, long code, StringElem p1, StringElem p2, int reduceType) {
    log.debug("Begin: basePartitionWin size1: " + objs1.size() + " size2: " + objs2.size());
    int r = 0;

    int o1StartIndx = 0;
    int o1EndIndx = objs1.size() - 1;

    ArrayList<StringElem> o1WinP1 = new ArrayList<StringElem>();
    ArrayList<StringElem> o1WinP2 = new ArrayList<StringElem>();

    double o1StartDist = (objs1.get(o1StartIndx).getJaccardDistanceBetween(p1) - objs1.get(o1StartIndx).getJaccardDistanceBetween(p2)) / 2;
    double o1EndDist = (objs1.get(o1EndIndx).getJaccardDistanceBetween(p1) - objs1.get(o1EndIndx).getJaccardDistanceBetween(p2)) / 2;
    while (o1StartIndx < o1EndIndx) {

      while (o1EndDist > r && o1StartIndx < o1EndIndx) {//closest to p2
        if (o1EndDist <= (r + (eps + errorAdjsmt))) {
          o1WinP1.add(objs1.get(o1EndIndx));
        }
        o1EndIndx--;
        o1EndDist = (objs1.get(o1EndIndx).getJaccardDistanceBetween(p1) - objs1.get(o1EndIndx).getJaccardDistanceBetween(p2)) / 2;
      }

      while (o1StartDist <= r && o1StartIndx < o1EndIndx) {//closest to p1
        if (o1StartDist >= (r - (eps + errorAdjsmt))) {
          o1WinP2.add(objs1.get(o1StartIndx));
        }
        o1StartIndx++;
        o1StartDist = (objs1.get(o1StartIndx).getJaccardDistanceBetween(p1) - objs1.get(o1StartIndx).getJaccardDistanceBetween(p2)) / 2;
      }

      if (o1StartIndx < o1EndIndx) {
        if (o1EndDist >= (r - (eps + errorAdjsmt))) {
          o1WinP2.add(objs1.get(o1EndIndx));
        }
        if (o1StartDist <= (r + (eps + errorAdjsmt))) {
          o1WinP1.add(objs1.get(o1StartIndx));
        }

        //exchange positions//
        StringElem temp = objs1.get(o1StartIndx);
        objs1.set(o1StartIndx, objs1.get(o1EndIndx));
        objs1.set(o1EndIndx, temp);

        o1StartIndx++;
        o1StartDist = (objs1.get(o1StartIndx).getJaccardDistanceBetween(p1) - objs1.get(o1StartIndx).getJaccardDistanceBetween(p2)) / 2;

        o1EndIndx--;
        o1EndDist = (objs1.get(o1EndIndx).getJaccardDistanceBetween(p1) - objs1.get(o1EndIndx).getJaccardDistanceBetween(p2)) / 2;

      }

    }//end outer while loop

    if (o1StartIndx == o1EndIndx) {
      if (o1EndDist > r && o1EndDist <= (r + (eps + errorAdjsmt))) {
        o1WinP1.add(objs1.get(o1EndIndx));
      }

      if (o1StartDist <= r && o1StartDist >= (r - (eps + errorAdjsmt))) {
        o1WinP2.add(objs1.get(o1StartIndx));
      }

      if (o1EndDist > r) {
        o1EndIndx--;
      }
    }

    int o2StartIndx = 0;
    int o2EndIndx = objs2.size() - 1;

    ArrayList<StringElem> o2WinP1 = new ArrayList<StringElem>();
    ArrayList<StringElem> o2WinP2 = new ArrayList<StringElem>();

    double o2StartDist = (objs2.get(o2StartIndx).getJaccardDistanceBetween(p1) - objs2.get(o2StartIndx).getJaccardDistanceBetween(p2)) / 2;
    double o2EndDist = (objs2.get(o2EndIndx).getJaccardDistanceBetween(p1) - objs2.get(o2EndIndx).getJaccardDistanceBetween(p2)) / 2;
    while (o2StartIndx < o2EndIndx) {

      while (o2EndDist > r && o2StartIndx < o2EndIndx) {//closest to p2
        if (o2EndDist <= (r + (eps + errorAdjsmt))) {
          o2WinP1.add(objs2.get(o2EndIndx));
        }
        o2EndIndx--;
        o2EndDist = (objs2.get(o2EndIndx).getJaccardDistanceBetween(p1) - objs2.get(o2EndIndx).getJaccardDistanceBetween(p2)) / 2;
      }

      while (o2StartDist <= r && o2StartIndx < o2EndIndx) {//closest to p1
        if (o2StartDist >= (r - (eps + errorAdjsmt))) {
          o2WinP2.add(objs2.get(o2StartIndx));
        }
        o2StartIndx++;
        o2StartDist = (objs2.get(o2StartIndx).getJaccardDistanceBetween(p1) - objs2.get(o2StartIndx).getJaccardDistanceBetween(p2)) / 2;
      }

      if (o2StartIndx < o2EndIndx) {
        if (o2EndDist >= (r - (eps + errorAdjsmt))) {
          o2WinP2.add(objs2.get(o2EndIndx));
        }
        if (o2StartDist <= (r + (eps + errorAdjsmt))) {
          o2WinP1.add(objs2.get(o2StartIndx));
        }

        //exchange positions//
        StringElem temp = objs2.get(o2StartIndx);
        objs2.set(o2StartIndx, objs2.get(o2EndIndx));
        objs2.set(o2EndIndx, temp);
                                      //////////////////////

        o2StartIndx++;
        o2StartDist = (objs2.get(o2StartIndx).getJaccardDistanceBetween(p1) - objs2.get(o2StartIndx).getJaccardDistanceBetween(p2)) / 2;

        o2EndIndx--;
        o2EndDist = (objs2.get(o2EndIndx).getJaccardDistanceBetween(p1) - objs2.get(o2EndIndx).getJaccardDistanceBetween(p2)) / 2;

      }

    }//end outer while loop

    if (o2StartIndx == o2EndIndx) {
      if (o2EndDist > r && o2EndDist <= (r + (eps + errorAdjsmt))) {
        o2WinP1.add(objs2.get(o2EndIndx));
      }

      if (o2StartDist <= r && o2StartDist >= (r - (eps + errorAdjsmt))) {
        o2WinP2.add(objs2.get(o2StartIndx));
      }

      if (o2EndDist > r) {
        o2EndIndx--;
      }
    }

    //log.debug("trim: basePartitionWin");
    o1WinP1.trimToSize();
    o1WinP2.trimToSize();
    o2WinP1.trimToSize();
    o2WinP2.trimToSize();

    //log.debug("Call baseQuickJoinWin: basePartitionWin");
    baseQuickJoinWin(o1WinP1.subList(0, o1WinP1.size()), o2WinP2.subList(0, o2WinP2.size()), eps, context, code, reduceType);
    baseQuickJoinWin(o1WinP2.subList(0, o1WinP2.size()), o2WinP1.subList(0, o2WinP1.size()), eps, context, code, reduceType);
    baseQuickJoinWin(objs1.subList(0, o1EndIndx + 1), objs2.subList(0, o2EndIndx + 1), eps, context, code, reduceType);
    baseQuickJoinWin(objs1.subList(o1EndIndx + 1, objs1.size()), objs2.subList(o2EndIndx + 1, objs2.size()), eps, context, code, reduceType);
  }
  
  private static void baseNestedLoop(List<StringElem> objs1, List<StringElem> objs2, double eps, Context context, long code, int reduceType) {
    boolean isSelfJoin = false;
    if (objs2 == null) {
      objs2 = objs1; // the self-join case
      isSelfJoin = true;
    }
    
    int outerCounter = 0;
    for (StringElem outerElem : objs1) {
      int innerCounter;
      if (isSelfJoin) {
        innerCounter = outerCounter + 1; // prevent duplicates
      } else {
        innerCounter = 0;
      }
      for (; innerCounter < objs2.size(); innerCounter++) { // StringElem innerElem : objs2
        StringElem innerElem = objs2.get(innerCounter);
        if (code == -1) {
          boolean tmp;
          if (reduceType == 0) { // base:
            tmp = innerElem.getKey() != outerElem.getKey();
          } else { // window:
            tmp = innerElem.getPrevPartition() != outerElem.getPrevPartition();
          }
          if (tmp) {
            writeIfSimilar(innerElem, outerElem, context, eps);
          }
        } else {
          boolean tmp = true;
          if (reduceType == 1) { // window:
            tmp = innerElem.getPrevPartition() != outerElem.getPrevPartition();
          }
          if (outerElem.partitionID != innerElem.partitionID && tmp) {
            writeIfSimilar(innerElem, outerElem, context, eps);
          }
        }//end it is a window
      } //end inner loop
      outerCounter++;
    } //end outer loop
  }
  
  private static void writeIfSimilar(StringElem innerElem, StringElem outerElem, Context context, double eps) {
    double diffDist = outerElem.getJaccardDistanceBetween(innerElem);
    //log.debug("BR Checking: Elem: " + outerElem.toString() + " Against: " + innerElem.toString() + " With Distance: " + diffDist);
    if (diffDist <= eps) { //checks if the difference in distance is less than eps
      try {
        context.write(new Text(outerElem.getKey() + ""), new Text(innerElem.getKey() + " " + (1 - diffDist)));
      } catch (IOException e) {
        log.error(e);
      } catch (InterruptedException e) {
        log.error(e);
      }
    }
  }
  
  private static void baseNestedLoop(List<StringElem> objs, double eps, Context context, long code, int reduceType) {
    baseNestedLoop(objs, null, eps, context, code, reduceType);
  }//end baseNestedLoop
  
  public static class CloudJoinBaseReducer extends Reducer<CloudJoinKey, StringElem, Text, Text> {
    double eps;
    long memory;
    long itr;
    String outDir;
  
    @Override
    public void setup(Context context) {
      String s = context.getConfiguration().get("eps", "");
      if ("".equals(s)) {
        log.error("BR Error: Unable to get eps from configuration");
      }
      eps = Double.parseDouble(s);
      s = context.getConfiguration().get("memory", "");
      if ("".equals(s)) {
        log.error("BR Error: Unable to get memory from configuration");
      }
      memory = Long.parseLong(s);
      s = context.getConfiguration().get("itr", "");
      if ("".equals(s)) {
        log.error("BR Error: Unable to get itr from configuration");
      }
      itr = Long.parseLong(s);
      outDir = context.getConfiguration().get("outDir", "");
      if ("".equals(outDir)) {
        log.error("BR Error: Unable to get outDir from configuration");
      }
    }
    
    @Override
    public void reduce(CloudJoinKey key, Iterable<StringElem> values, Context context) {
      genericReducer(key, values, context, eps, memory, itr, outDir, 0);
    }//end reduce	
  }//end CloudJoinBaseReducer

  public static class CloudJoinWindowReducer extends Reducer<CloudJoinKey, StringElem, Text, Text> {
    double eps;
    long memory;
    long itr;
    String outDir;
  
    @Override
    public void setup(Context context) {
      String s = context.getConfiguration().get("eps", "");
      if ("".equals(s)) {
        log.error("BR Error: Unable to get eps from configuration");
      }
      eps = Double.parseDouble(s);
      s = context.getConfiguration().get("memory", "");
      if ("".equals(s)) {
        log.error("BR Error: Unable to get memory from configuration");
      }
      memory = Long.parseLong(s);
      s = context.getConfiguration().get("itr", "");
      if ("".equals(s)) {
        log.error("BR Error: Unable to get itr from configuration");
      }
      itr = Long.parseLong(s);
      outDir = context.getConfiguration().get("outDir", "");
      if ("".equals(outDir)) {
        log.error("BR Error: Unable to get outDir from configuration");
      }
    }
    
    @Override
    public void reduce(CloudJoinKey key, Iterable<StringElem> values, Context context) {
      System.out.println("CloudJoinWindowReducer started");
      genericReducer(key, values, context, eps, memory, itr, outDir, 1);
      System.out.println("CloudJoinWindowReducer ended");
    }//end reduce	
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new MRSimJoinJob(), args);
  }

  @Override
  public int run(String[] args) throws Exception {
    System.out.println("MRSimJoin");
    log.setLevel(Level.ALL);
    ParameterParser pp = new ParameterParser(args);
    
    double eps;
    int numP = 2;
    int numReduces = 1;
    String inputDir, outputDir;
    long memory = 33554432; //32MB

    eps = 1 - new Double(pp.getTheta()); // we use similarity as input. This algorithm uses distance
    inputDir = pp.getInput() + "/";
    outputDir = pp.getOutput() + "/";
    if (pp.getMemory() != 0) {
      memory = new Integer(pp.getMemory()) * 1048576; // convert MB into Bytes
    }
    if (pp.getNumPartitions() != 0) {
      numP = new Integer(pp.getNumPartitions());
    }
    if (pp.getNumReducers() != 0) {
      numReduces = new Integer(pp.getNumReducers());
    }
    
    PatternLayout appenderLayout = new PatternLayout();
    appenderLayout.setConversionPattern("%m%n"); // message + newline
//    log.getParent().removeAllAppenders();

    long itr = 0; // iteration counter

    String intermediate = outputDir + "INTERMEDIATE/";
    String processed = outputDir + "PROCESSED/";
    String pivDir = outputDir + "PIVOTS/";
    FileSystem filesystem = FileSystem.get(getConf());
    filesystem.delete(new Path(outputDir), true);

    long startTime = System.currentTimeMillis();
    while (true) {
      Configuration conf = new Configuration();
      conf.set("itr", "" + itr);
//      String logOutFile = outputDir.replace("file://", "") + itr + "log.txt";
//      FileAppender appender = new FileAppender(appenderLayout, logOutFile);
//      log.addAppender(appender);

      FileSystem fs = FileSystem.get(conf);
      Path inputPath;
      Path pivPath = new Path(pivDir + itr + "/pivots.txt");
      boolean isBaseRound = true;   ///need to determine if it is a base round or not
      
      if (itr == 0) {//first iteration, set input to primary input
        //make intermediate directory inside outputDir
        fs.mkdirs(new Path(intermediate));
        //make processed directory inside outputDir
        fs.mkdirs(new Path(processed));
        //set input path
        inputPath = new Path(inputDir);
      } else {
	//get first input directory in INTERMEDIATE
        // use listStatus(INTERMEDIATE)
        FileStatus[] fileStat = fs.listStatus(new Path(intermediate));
        System.out.println("fileStat length: " + fileStat.length);
        if (fileStat.length >= 1) {
          //set jobInputDir to first directory returned
          inputPath = fileStat[0].getPath();
          System.out.println("Input Path: " + inputPath.toString());
        } else {
          System.out.println("No input path (any more)");
          break;
        }

        int lastIndex = inputPath.toString().lastIndexOf("intermediate_output_"); // in diesem intermediate Dateinamen ist codiert, ob das jetzt eine Base Partition oder eine Window Partition ist
        String subBase = inputPath.toString().substring(lastIndex + 20);
        StringTokenizer st = new StringTokenizer(subBase, "_");
        int tokenCount = st.countTokens();
        if (tokenCount == 2)//if tokenCount = 2, then base partition
        {
          System.out.println("CheckRound: Base Round");
          isBaseRound = true; //it's a base round, set to true and continue
        } else if (tokenCount == 3 || tokenCount == 4) { // window partion
          System.out.println("CheckRound: Window Round");
          isBaseRound = false; //it's a window round, set to false and continue
          st.nextToken();
          String v = st.nextToken();
          String w = st.nextToken();
          conf.set("V", v);
          conf.set("W", w);
        } else {
          System.out.println("Directory Input Error");
          break;
        }
      }

      conf.set("eps", Double.toString(eps)); //assign string for eps to conf
      conf.set("memory", Long.toString(memory)); //assigns the variable mem to conf
      Object[] pivots = getPivots(inputPath, numP); //generate pivot points from inputPath directory
      FSDataOutputStream out = fs.create(pivPath); //write pivots to file      
      for (Object pivot : pivots) {
        splitAndWrite(((Text) pivot).toString() + '\n', out);
//        out.writeBytes(((Text) pivot).toString() + '\n');
      }
      out.close();

      Job job;
      if (isBaseRound) { //if it is a Base round
        //execute base round MR on jobInputDir
        System.out.println("Base Round");
        //creates job
        job = new Job(conf, "Cloud Join Base Round: Itr " + itr);
        //sets mapper class
        job.setMapperClass(CloudJoinBaseMapper.class);
        //sets Partitioner Class
        job.setPartitionerClass(CloudJoinBasePartitioner.class);
        //sets Sorting Comparitor class for job
        job.setSortComparatorClass(CloudJoinBaseGrouper.class);
        //Sets grouper class
        job.setGroupingComparatorClass(CloudJoinBaseGrouper.class);
        //Set Reducer class
        job.setReducerClass(CloudJoinBaseReducer.class);
      } else { //else it is a window
        //execute window round MR on jobInputDir
        System.out.println("Window Round");
        //creates job
        job = new Job(conf, "Cloud Join Window Round: Itr " + itr);
        //sets mapper class
        job.setMapperClass(CloudJoinWindowMapper.class);
        //sets Partitioner Class
        job.setPartitionerClass(CloudJoinWindowPartitioner.class);
        //sets Sorting Comparitor class for job
        job.setSortComparatorClass(CloudJoinWindowGrouper.class);
        //Sets grouper class
        job.setGroupingComparatorClass(CloudJoinWindowGrouper.class);
        //Set Reducer class
        job.setReducerClass(CloudJoinWindowReducer.class);
      }
      // common job settings for both base and window rounds:
      job.setNumReduceTasks(numReduces);
      //add file to cache
      String cacheFileName = pivPath.toString() + "#pivots";
      job.addCacheFile(new URI(cacheFileName)); // pivPath.toUri()
//      DistributedCache.addCacheFile(pivPath.toUri(), job.getConfiguration());
      job.getConfiguration().set("outDir", intermediate);
      job.setJarByClass(MRSimJoinJob.class);
      //sets map output key/value types
      job.setMapOutputKeyClass(CloudJoinKey.class);
      job.setMapOutputValueClass(StringElem.class);
      // specify output types
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class);
      //sets input format
      job.setInputFormatClass(TextInputFormat.class);
      // specify input and output DIRECTORIES (not files)
      FileInputFormat.addInputPath(job, inputPath);
      FileOutputFormat.setOutputPath(job, new Path(outputDir + "/OUTPUT/" + itr + "/"));
      
      //start and wait till it finishes
      job.waitForCompletion(true);

      if (itr > 0) {
	// change jobInputDir from intermediate to processed
        String dir = inputPath.toString();
        dir = dir.replace("INTERMEDIATE", "PROCESSED");
        dir = dir.replace("intermediate_output", itr + "_intermediate_output");
        Path newPath = new Path(dir);
        fs.rename(inputPath, newPath);
      }

      //increment itr for next round
      itr++;
//      log.removeAppender(appender);
    }

    long endTime = System.currentTimeMillis();

    System.out.println("Total Run Time: " + (endTime - startTime));
    return 0;
  }

  Object[] getPivots(Path input, int numPivs) throws IOException {
    JobConf job = new JobConf();
    job.setInputFormat(KeyValueTextInputFormat.class);
    job.setMapOutputKeyClass(Text.class);
    org.apache.hadoop.mapred.FileInputFormat.addInputPath(job, input);
    final KeyValueTextInputFormat inf = (KeyValueTextInputFormat) job.getInputFormat();
    InputSampler.Sampler<Text, Text> sampler = new InputSampler.RandomSampler<Text, Text>(1.0, numPivs, 100);
    Object[] samples = sampler.getSample(inf, job);
    return samples;
  }

}
