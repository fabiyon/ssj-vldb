package de.huberlin.textualsimilarityhadoop;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class VsmartReplicationJob {

  public static class ReplicationMapper extends Mapper<Object, Text, IntWritable, LongWritable> {
    private final IntWritable outKey = new IntWritable();
    private final LongWritable outValue = new LongWritable();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String[] valueArr = value.toString().split("\\s+");
      if (valueArr.length > 1) { // ignore empty records:
        String[] tokenArr = valueArr[1].split(",");
        long c = (Long.parseLong(valueArr[0]) << 32) | ((long)tokenArr.length & 0xFFFFFFFL); // rid recordCardinality: consolidate both integers in one long
        outValue.set(c);
//        outValue.set(valueArr[0] + " " + tokenArr.length); // rid recordCardinality
        for (String token : tokenArr) {
          outKey.set(Integer.parseInt(token));
          context.write(outKey, outValue);
        }    
      }
    }
  }
  
  public static class CandidateReducer extends Reducer<IntWritable, LongWritable, BytesWritable, BytesWritable> {
    private final BytesWritable outKey = new BytesWritable();
    private BytesWritable outVal = new BytesWritable();

    @Override
    public void reduce(IntWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
      Iterator<LongWritable> it = values.iterator(); // rid recordCardinality

      HashMap<Integer, Integer> oldRecords = new HashMap();
      
      while (it.hasNext()) {
        long combinedKey = it.next().get();
        int currentRid = (int)(combinedKey >> 32);
        int currentRecordCardinality = (int)combinedKey;
        
//        String[] currentArr = it.next().toString().split(" ");
//        Long currentRid = Long.parseLong(currentArr[0]);
//        Integer currentRecordCardinality = Integer.parseInt(currentArr[1]);
        
        
        for(Map.Entry<Integer,Integer> entry : oldRecords.entrySet()) {
          Integer candidateRid = entry.getKey();
          Integer candidateRecordCardinality = entry.getValue();
          
          byte[] outBytes = new byte[12]; // 3x integer
          byte[] rid1 = new byte[0];
          byte[] rid2 = new byte[0];
          byte[] sumCardinality;
                  
          if (currentRid < candidateRid) {
            rid1 = ByteBuffer.allocate(4).putInt(currentRid).array();
            rid2 = ByteBuffer.allocate(4).putInt(candidateRid).array();
//            outKey.set(currentRid + " " + candidateRid + " " + (currentRecordCardinality + candidateRecordCardinality));
//            outValue.set();
          } else if (candidateRid < currentRid) {
            rid1 = ByteBuffer.allocate(4).putInt(candidateRid).array();
            rid2 = ByteBuffer.allocate(4).putInt(currentRid).array();
//            outKey.set(candidateRid + " " + currentRid + " " + (currentRecordCardinality + candidateRecordCardinality));
//            outValue.set();
          }
          sumCardinality = ByteBuffer.allocate(4).putInt(currentRecordCardinality + candidateRecordCardinality).array();
          System.arraycopy(rid1, 0, outBytes, 0, 4);
          System.arraycopy(rid2, 0, outBytes, 4, 4);
          System.arraycopy(sumCardinality, 0, outBytes, 8, 4);
          outVal = new BytesWritable(outBytes);
          context.write(outKey, outVal);
        }
        oldRecords.put(currentRid, currentRecordCardinality);
      }
      
    }
  }

}
