package de.huberlin.textualsimilarityhadoop;


/**
 * Dieser Join leitet sich vom Elsayed-Join ab und ist nur dazu gibt, statistische Infos zu berechnen
 * für Visualisierungen.
 * 
 */
import com.google.common.primitives.Bytes;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileAsBinaryInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileAsBinaryOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class StatisticsJoinJob extends Configured implements Tool {
  private static final Logger log = Logger.getLogger(StatisticsJoinJob.class);
  
  public static class IndexMapper extends Mapper<LongWritable, Text, IntWritable, BytesWritable> {
    IntWritable outKey = new IntWritable();
    BytesWritable outVal;
    byte[] outBytes = new byte[12]; // long 8 bytes, int 4 bytes
    
    @Override
    public void map(LongWritable inputKey, Text inputValue, Context context) throws IOException, InterruptedException {
      String[] textArr = inputValue.toString().split("\\s+");
      if (textArr.length < 2) {
        return;
      }
      int[] tokens = this.toTokenArr(textArr[1]);
      Long key = Long.parseLong(textArr[0]);
      byte[] keyBytes = ByteBuffer.allocate(8).putLong(key).array();
      byte[] numOfTokensBytes = ByteBuffer.allocate(4).putInt(tokens.length).array();

      System.arraycopy(keyBytes, 0, outBytes, 0, 8);
      System.arraycopy(numOfTokensBytes, 0, outBytes, 8, 4);
      outVal = new BytesWritable(outBytes);
      
      for (int i = 0; i < tokens.length; i++) {
        outKey.set(tokens[i]);
        context.write(outKey, outVal); // tokenId, <docId, numberOfTokens>
      }
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
  }
  
  public static class IndexReducer extends Reducer<IntWritable, BytesWritable, BytesWritable, BytesWritable> {
    BytesWritable outKey = new BytesWritable();
    BytesWritable outVal;
    
    @Override
    public void reduce(IntWritable key, Iterable<BytesWritable> values, Context context) throws IOException, InterruptedException {
      int outCount = 0;
      // we do not need the key (the term) anymore, so we don't do anything with it. <<< für E2E wärs aber wichtig
      Iterator<BytesWritable> it = values.iterator();
      ArrayList<Byte> byteList = new ArrayList();
      byte[] currentBytes;
      while (it.hasNext()) {
        currentBytes = it.next().getBytes();
        for (int i = 0; i < 12; i++) {
          byteList.add(currentBytes[i]);
        }
        outCount++;
      } 
      
      if (outCount > 1) { // we can safely filter terms that only occur once <<< auch blöd für E2E
        byte[] outArr = Bytes.toArray(byteList);
        outVal = new BytesWritable(outArr);
        context.write(outKey, outVal);
      }
    }
  }
  
  public static class JoinMapper extends Mapper<BytesWritable, BytesWritable, BytesWritable, BytesWritable> {
    BytesWritable outKey = new BytesWritable();
    BytesWritable outVal = new BytesWritable();
    byte[] outBytes = new byte[16]; // 2x long 8 bytes
    byte[] outValBytes = new byte[12]; // 3x int 4 bytes
    ArrayList<Integer> sizeList = new ArrayList();
    ArrayList<Long> keyList = new ArrayList();
    Long key1;
    Long key0;
    Long tmp;
    
    @Override
    protected void setup(Context context) {
      byte[] one = ByteBuffer.allocate(4).putInt(1).array();
      int counter = 0;
      for (byte b : one) {
        outValBytes[counter++] = b;
      }
    }
    
    @Override
    public void map(BytesWritable inputKey, BytesWritable inputValue, Context context) throws IOException, InterruptedException {
      byte[] input = inputValue.getBytes(); // be careful! The byte array length is usually larger than its intended length which can be found in inputValue.getLength()
      sizeList.clear();
      keyList.clear();
      
      for (int i = 0; i < inputValue.getLength(); i += 12) {
        ByteBuffer keyBuffer = ByteBuffer.allocate(8);
        keyBuffer.put(input, i, 8);
        keyBuffer.flip();
        Long currentKey = keyBuffer.getLong();
        ByteBuffer recordSizeBuffer = ByteBuffer.allocate(4);
        recordSizeBuffer.put(input, i + 8, 4);
        recordSizeBuffer.flip();
        Integer currentRecordSize = recordSizeBuffer.getInt();
        
        // join with all previous entries:
        for (int j = 0; j < sizeList.size(); j++) {
          key0 = currentKey;
          key1 = keyList.get(j);
          byte[] size0;
          byte[] size1;
          if (key0 > key1) {
            tmp = key1;
            key1 = key0;
            key0 = tmp;
            size0 = ByteBuffer.allocate(4).putInt(sizeList.get(j)).array();
            size1 = ByteBuffer.allocate(4).putInt(currentRecordSize).array();
          } else {
            size0 = ByteBuffer.allocate(4).putInt(currentRecordSize).array();
            size1 = ByteBuffer.allocate(4).putInt(sizeList.get(j)).array();
          }
          System.arraycopy(size0, 0, outValBytes, 4, 4);
          System.arraycopy(size1, 0, outValBytes, 8, 4);
          outVal.set(outValBytes, 0, 12);
          
          byte[] key0Bytes = ByteBuffer.allocate(8).putLong(key0).array();
          System.arraycopy(key0Bytes, 0, outBytes, 0, 8);
          byte[] key1Bytes = ByteBuffer.allocate(8).putLong(key1).array();
          System.arraycopy(key1Bytes, 0, outBytes, 8, 8);
          outKey.set(outBytes, 0, 16);
          context.write(outKey, outVal); // we could add a length filter here
        }
        sizeList.add(currentRecordSize);
        keyList.add(currentKey);
      }      
    }
  }
  
  public static class JoinCombiner extends Reducer<BytesWritable, BytesWritable, BytesWritable, BytesWritable> {
    BytesWritable outVal = new BytesWritable();
    byte[] outArr = new byte[12];
    
    private Integer getIntFromByteArr(byte[] input, int start) {
      ByteBuffer recordSizeBuffer = ByteBuffer.allocate(4);
      recordSizeBuffer.put(input, start, 4);
      recordSizeBuffer.flip();
      return recordSizeBuffer.getInt();
    }
    
    @Override
    public void reduce(BytesWritable key, Iterable<BytesWritable> values, Context context) throws IOException, InterruptedException {
      int overlap = 0;
      boolean copiedSizes = false;
      Iterator<BytesWritable> it = values.iterator();
      byte[] current;
      
      while (it.hasNext()) {
        current = it.next().getBytes(); // 3x integer: overlap, documentSize0, documentSize1
        overlap += getIntFromByteArr(current, 0);
        if (!copiedSizes) {
          System.arraycopy(current, 4, outArr, 4, 8);
          copiedSizes = true;
        }
      }
      
      byte[] overlapBytes = ByteBuffer.allocate(4).putInt(overlap).array();
      System.arraycopy(overlapBytes, 0, outArr, 0, 4);
      outVal.set(outArr, 0, 12);
      context.write(key, outVal);
    }
  }
  
  public static class JoinReducer extends Reducer<BytesWritable, BytesWritable, Text, NullWritable> {
//    LongWritable outKey = new LongWritable();
//    LongWritable outVal = new LongWritable();
    Text outKey = new Text();
    NullWritable outVal = NullWritable.get();
    double delta;

    @Override
    protected void setup(Context context) {
      delta = Double.parseDouble(context.getConfiguration().get("delta", ""));
    }
    
    private Integer getIntFromByteArr(byte[] input, int start) {
      ByteBuffer recordSizeBuffer = ByteBuffer.allocate(4);
      recordSizeBuffer.put(input, start, 4);
      recordSizeBuffer.flip();
      return recordSizeBuffer.getInt();
    }
    
    @Override
    public void reduce(BytesWritable key, Iterable<BytesWritable> values, Context context) throws IOException, InterruptedException {
      double overlap = 0;
      double size0 = 0;
      double size1 = 0;
      Iterator<BytesWritable> it = values.iterator();
      byte[] current;
      
      if (it.hasNext()) {
        current = it.next().getBytes(); // 3x integer: overlap, documentSize0, documentSize1
        overlap += getIntFromByteArr(current, 0);
        size0 = getIntFromByteArr(current, 4);
        size1 = getIntFromByteArr(current, 8);
      }
      
      while (it.hasNext()) {
        current = it.next().getBytes(); // 3x integer: overlap, documentSize0, documentSize1
        overlap += getIntFromByteArr(current, 0);
      }
      
      double similarity = overlap / (size0 + size1 - overlap);
      
//      if (similarity > delta) {
        byte[] keyBytes = key.getBytes();
        ByteBuffer key0Buffer = ByteBuffer.allocate(8);
        key0Buffer.put(keyBytes, 0, 8);
        key0Buffer.flip();
//        outKey.set(key0Buffer.getLong());
//
        ByteBuffer key1Buffer = ByteBuffer.allocate(8);
        key1Buffer.put(keyBytes, 8, 8);
        key1Buffer.flip();
//        outVal.set(key1Buffer.getLong());
//
//        context.write(outKey, outVal);
//      }
//      System.out.println(key0Buffer.getLong() + " " + key1Buffer.getLong() + " " + similarity);
        outKey.set(key0Buffer.getLong() + " " + key1Buffer.getLong() + " " + similarity);
        context.write(outKey, outVal);
    }
  }
  
  @Override
  public int run(String[] args) throws Exception {
    System.out.println("FullFiltering");
    log.setLevel(Level.ALL);
    
    ParameterParser pp = new ParameterParser(args);
    
    Configuration conf = this.getConf();
    conf.set("delta", pp.getTheta());
    // input format: 1 796956,798211,802627
    String inputPathString = pp.getInput();
    Path inputPath = new Path(inputPathString);
    String temporaryPathString = pp.getOutput() + "tmp1";
    Path temporaryPath = new Path(temporaryPathString);
    String outputPathString = pp.getOutput();
    Path outputPath = new Path(outputPathString);
    FileSystem filesystem = FileSystem.get(getConf());
    filesystem.delete(temporaryPath, true);
    filesystem.delete(outputPath, true);
    
    Job job1a = Job.getInstance(conf, "Step 1: Create inverted index");
    job1a.setJarByClass(StatisticsJoinJob.class);
    
    job1a.setMapperClass(IndexMapper.class);
    job1a.setMapOutputKeyClass(IntWritable.class);
    job1a.setMapOutputValueClass(BytesWritable.class);
    
    job1a.setReducerClass(IndexReducer.class);
    job1a.setOutputKeyClass(BytesWritable.class);
    job1a.setOutputValueClass(BytesWritable.class);
    
    
    FileInputFormat.addInputPath(job1a, inputPath);
    SequenceFileAsBinaryOutputFormat.setOutputPath(job1a, temporaryPath);
    job1a.setOutputFormatClass(SequenceFileAsBinaryOutputFormat.class);
    job1a.waitForCompletion(true);
    
    Job job2a = Job.getInstance(conf, "Step 2: Compute similarity of pairs");
    job2a.setJarByClass(StatisticsJoinJob.class);
    
    job2a.setMapperClass(JoinMapper.class);
    job2a.setMapOutputKeyClass(BytesWritable.class);
    job2a.setMapOutputValueClass(BytesWritable.class);
    
    job2a.setCombinerClass(JoinCombiner.class);
    
    job2a.setReducerClass(JoinReducer.class);
    job2a.setOutputKeyClass(Text.class);
    job2a.setOutputValueClass(NullWritable.class);
    
    SequenceFileAsBinaryInputFormat.addInputPath(job2a, temporaryPath);
    job2a.setInputFormatClass(SequenceFileAsBinaryInputFormat.class);
    FileOutputFormat.setOutputPath(job2a, outputPath);
    int returnCode = job2a.waitForCompletion(true) ? 0 : 1;

    filesystem.delete(temporaryPath, true);

    return returnCode;
  }
  
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new StatisticsJoinJob(), args);
  }
  
}