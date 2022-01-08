package de.huberlin.textualsimilarityhadoop;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class EndToEndTextOPRJJoiner {

  /**
   * input: similar pairs of rids: {< rid1 rid2 >} original input augmented with
   * rids: {<rid tokenInt,tokenInt,... originalText>}
   *
   */
  public static class OPRJEndMapper1 extends Mapper<Object, Text, LongWritable, Text> {

    private final LongWritable outKey = new LongWritable();
    private final Text outValue = new Text();
    HashMap<Integer, ArrayList<Integer>> ridPairs = new HashMap();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      // read token list for tokenization:
      String ridPairString = context.getConfiguration().get("ridPairs", "");
      try {
        Path pt = new Path(ridPairString);
        FileSystem fs = FileSystem.get(new Configuration());
        RemoteIterator<LocatedFileStatus> it = fs.listFiles(pt, true);
        while (it.hasNext()) {
          LocatedFileStatus lfs = it.next();
          if (lfs.isFile()) {
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(lfs.getPath())));
            String line = br.readLine();
            while (line != null) {
              String[] lineArr = line.split("\\s+");
              Integer rid1 = Integer.parseInt(lineArr[0]);
              Integer rid2 = Integer.parseInt(lineArr[1]);
              ArrayList<Integer> ridList1 = ridPairs.get(rid1);
              if (ridList1 == null) {
                ridList1 = new ArrayList();
              }
              ridList1.add(rid2);
              ridPairs.put(rid1, ridList1);
              
              ArrayList<Integer> ridList2 = ridPairs.get(rid2);
              if (ridList2 == null) {
                ridList2 = new ArrayList();
              }
              ridList2.add(rid1);
              ridPairs.put(rid2, ridList2);
              line = br.readLine();
            }
          }
        }
      } catch (Exception e) {
      }

    }

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String[] valueArr = value.toString().split("\\s+");
      Integer key1 = Integer.parseInt(valueArr[0]);
      
      ArrayList<Integer> key2List = ridPairs.get(key1);
      if (key2List != null) {
        for (Integer key2 : key2List) {
          if (key2 != null) {
            if (key1 < key2) {
//              outKey.set(key1 + "_" + key2);
              outKey.set(((long)key1 << 32) | ((long)key2 & 0xFFFFFFFL));
            } else {
//              outKey.set(key2 + "_" + key1);
              outKey.set(((long)key2 << 32) | ((long)key1 & 0xFFFFFFFL));
            }

            String origText = "";
            for (int i = 2; i < valueArr.length; i++) {
              origText += valueArr[i];
            }
            outValue.set(origText);
            context.write(outKey, outValue);
          }
        }
      }
    }
  }
  
  public static class OPRJEndReducer1 extends Reducer<LongWritable, Text, Text, NullWritable> {
    private final Text outKey = new Text();
    private final NullWritable outValue = NullWritable.get();

    @Override
    public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      Iterator<Text> it = values.iterator();

      String text0 = it.next().toString();
      if (it.hasNext()) { // this should always be the case...
        String text1 = it.next().toString();

        outKey.set(text0 + " " + text1);
        context.write(outKey, outValue);
      } else {
        System.out.println("Corrupted data!!!");
      }
    }
  }
}
