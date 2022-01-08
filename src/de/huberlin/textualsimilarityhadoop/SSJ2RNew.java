package de.huberlin.textualsimilarityhadoop;

import aps.io.GenericKey;
import aps.io.GenericValue;
import aps.io.HalfPair;
import aps.io.IndexItem;
import aps.io.IndexItemArrayWritable;
import aps.io.VectorComponent;
import aps.io.VectorComponentArrayWritable;
import edu.uci.ics.fuzzyjoin.similarity.SimilarityFiltersJaccard;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @author fabi
 */
public class SSJ2RNew extends Configured implements Tool {
  
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new SSJ2RNew(), args);
  }

  @Override
  public int run(String[] args) throws Exception {
    System.out.println("SSJ-2R");

    ParameterParser pp = new ParameterParser(args);
    float threshold = Float.parseFloat(pp.getTheta());
    String inputPathString = pp.getInput();
    Path inputPath = new Path(inputPathString);
    String outputPathString = pp.getOutput();
    Path outputPath = new Path(outputPathString);
    Path indexPath = new Path(inputPathString + "-index-" + threshold);
    Path indexOnlyPath = new Path(indexPath, "part*"); // chooses only the files with "part" in the name
    
    Configuration conf = new Configuration();
    conf.setFloat("theta", threshold);
    conf.set("indexPath", indexPath.toString()); // <<<<<<<<<reicht das oder muss das toURI sein...?
    FileSystem filesystem = FileSystem.get(getConf());
    
    filesystem.delete(outputPath, true);
    filesystem.delete(indexPath, true);
    
    System.out.println("SSJ First Step");
    Job job1 = Job.getInstance(conf, "SSJ First Step");
    job1.setJarByClass(SSJ2RNew.class);
    FileInputFormat.addInputPath(job1, inputPath);
    FileOutputFormat.setOutputPath(job1, indexPath);
    
    job1.setOutputFormatClass(SequenceFileOutputFormat.class); ///<<<<<<<<<<<<<<<<<<<<<
    job1.setMapperClass(IndexerMapper.class);
    job1.setMapOutputKeyClass(IntWritable.class); // tokenId
    job1.setMapOutputValueClass(IndexItem.class); // IndexItem<RecordId, recordLength>

    job1.setReducerClass(IndexerReducer.class);
    job1.setOutputKeyClass(IntWritable.class); // tokenId
    job1.setOutputValueClass(IndexItemArrayWritable.class); // IndexItemArrayWritable = { IndexItem1, IndexItem2 }

    // assuming input is sorted according to the key (vectorID) so that the part files are locally sorted
    MultipleOutputs.addNamedOutput(job1, "pruned", SequenceFileOutputFormat.class, IntWritable.class, VectorComponentArrayWritable.class); // recordId, tokens

    job1.waitForCompletion(true);
    
    System.out.println("SSJ Second Step");
    Job job2 = Job.getInstance(conf, "SSJ Second Step");
    job2.setJarByClass(SSJ2RNew.class);
    
    // Der indexOnlyPath filtert die Pruned-Daten raus
    MultipleInputs.addInputPath(job2, indexOnlyPath, SequenceFileInputFormat.class, SimilarityMapperIndex.class);
//    MultipleInputs.addInputPath(job2, indexOnlyPath, TextInputFormat.class, SimilarityMapperIndexNew.class);
    MultipleInputs.addInputPath(job2, inputPath, TextInputFormat.class, SimilarityMapperInput.class); // LPDs. Mit diesem Input alleine wird der Reducer gerufen. Ohne wirft der Job einen Fehler.
    job2.setMapOutputKeyClass(GenericKey.class);
    job2.setMapOutputValueClass(GenericValue.class);
    
    if (job2.getNumReduceTasks() < 2) {
      job2.setNumReduceTasks(2); // this needs to be set in local environment to trigger the non-default partitioner etc. 
    }
    job2.setPartitionerClass(GenericKey.StripePartitioner.class); // this is only called if there is more than 1 reducer configured!
    job2.setSortComparatorClass(GenericKey.Comparator.class);
    job2.setGroupingComparatorClass(GenericKey.PrimaryComparator.class);
    
    job2.setCombinerClass(SimilarityCombiner.class);
    job2.setReducerClass(SimilarityReducer.class);
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(NullWritable.class);
    
    FileOutputFormat.setOutputPath(job2, outputPath);

    int returnval = job2.waitForCompletion(true) ? 0 : 1;
//    filesystem.delete(indexPath, true);
    
    return returnval;
  }
  
  public static class VectorTokenizer {
    public static String[] toStringArr(Text rawInput) {
      String[] textArr = rawInput.toString().split("\\s+");
      return textArr;
    }
    
    public static VectorComponent[] toVectorArr(String[] textArr) {
      String[] tokenStringArr = textArr[1].split(",");
      VectorComponent[] tokenArr = new VectorComponent[tokenStringArr.length];
      int count = 0;
      for (String token : tokenStringArr) {
        if (!token.equals("")) {
          tokenArr[count++] = new VectorComponent(Integer.parseInt(token));
        }
      }
      Arrays.sort(tokenArr); // necessary due to our generated datasets
      return tokenArr;
    }
  }
  
  public static class IndexerMapper extends Mapper<LongWritable, Text, IntWritable, IndexItem> {
    private MultipleOutputs mos;
    private final IntWritable outKey = new IntWritable();
    private final IndexItem outValue = new IndexItem();
    private final IntWritable vectorID = new IntWritable();
    private SimilarityFiltersJaccard filter;
    VectorComponentArrayWritable vcaw = new VectorComponentArrayWritable();

    @Override
    protected void setup(Context context) {
      Configuration conf = context.getConfiguration();
      mos = new MultipleOutputs(context);
      filter = new SimilarityFiltersJaccard(conf.getFloat("theta", 1), 1);
    }

    @Override
    public void map(LongWritable key, Text inputValue, Context context) throws IOException, InterruptedException {
      String[] textArr = VectorTokenizer.toStringArr(inputValue);
      if (textArr.length < 2) {
        return;
      }
      int keyInt = Integer.parseInt(textArr[0]);
      VectorComponent[] vcarray = VectorTokenizer.toVectorArr(textArr); // <<<<<<<<<<<<<<<< NullPointerException auf dem Cluster
      vectorID.set(keyInt);
      
      int prefixLength = filter.getPrefixLength(vcarray.length); // we need to take the probe prefix here
      int prunedNumber = vcarray.length - prefixLength;

      // save the pruned part of the vector
      if (prunedNumber > 0) {
        vcaw.set(Arrays.copyOf(vcarray, prunedNumber));
        mos.write("pruned", vectorID, vcaw);
      }

      for (int i = 0; i < vcarray.length; i++) {
        // index the remaining part
        if (i >= prunedNumber) {
          outKey.set(vcarray[i].getID());
          outValue.set(vectorID.get()); // outValue wird immer sauber neu beschrieben; kein Problem hier
          outValue.setMaxIndexed(vcarray[prunedNumber].getID()); // equals b_i and b_j. Needed to know which full record to join in the SimilarityReducer
          outValue.setVectorLength(vcarray.length);
          context.write(outKey, outValue);
        }
      }
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
      mos.close();
    }
  }
  
  
  /**
   * Computes the index: {tokenId, {IndexItem}}
   */
  public static class IndexerReducer extends Reducer<IntWritable, IndexItem, IntWritable, IndexItemArrayWritable> {

    @Override
    public void reduce(IntWritable key, Iterable<IndexItem> iterableValues, Context context) throws IOException, InterruptedException {     
      List<IndexItem> list = new LinkedList();
      Iterator<IndexItem> values = iterableValues.iterator();
      while (values.hasNext()) {
        list.add(new IndexItem(values.next())); // deep copy. also contains vectorLength which we need later
      }
      if (list.size() > 1) { // do not output single valued posting lists (macht Sinn, verlorengehen tut da nichts)
        IndexItem buffer[] = list.toArray(new IndexItem[list.size()]); // it's not necessary to sort this array
        context.write(key, new IndexItemArrayWritable(buffer));
//        if (buffer.length > 100) {
//          System.out.println(key.get() + " " + buffer.length);
//        }
      }
    }

  }

  
  public static class SimilarityMapperInput extends Mapper<LongWritable, Text, GenericKey, GenericValue> { // IntWritable, VectorComponentArrayWritable
    private final GenericKey outKey = new GenericKey();
    private final GenericValue outValue = new GenericValue();
    private final VectorComponentArrayWritable value = new VectorComponentArrayWritable();

    @Override
    public void map(LongWritable key, Text inputValue, Context context) throws IOException, InterruptedException {
      String[] textArr = VectorTokenizer.toStringArr(inputValue);
      if (textArr.length < 2) {
        return;
      }
      Integer keyInt = Integer.parseInt(textArr[0]);
      VectorComponent[] vcarray = VectorTokenizer.toVectorArr(textArr);
      value.set(vcarray);

      outKey.set(keyInt); // <<<< das setzt den primary Key. Der Secondary ist Sternchen! im Key wird der secondary in dem Fall -1
      outValue.set(value);
      context.write(outKey, outValue);
    }
  }

//  public static class SimilarityMapperIndexNew extends Mapper<LongWritable, Text, GenericKey, GenericValue> {
//
//    private final GenericKey outKey = new GenericKey();
//    private final GenericValue outValue = new GenericValue();
//    private final HalfPair payload = new HalfPair();
//
//    @Override
//    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//      try {
//        IndexItem[] postingList = value.toIndexItemArray();
//        for (int i = 1; i < postingList.length; i++) { // NLJ to produce candidate pairs:
//          for (int j = 0; j < i; j++) {
//            IndexItem x = postingList[i];
//            IndexItem y = postingList[j];
//
////            if (x.vectorLength() > y.vectorLength() || (x.vectorLength() == y.vectorLength() && x.getID() > y.getID())) { // erstes Vertauschkriterium für Korrektheit: Länge. Der kürzere muss vorne stehen, sonst verlieren wir Resultate. Falls sie gleich lang sind nehmen wir als Ordnung die ID.
//            if ((x.getMaxIndexed() < y.getMaxIndexed()) || (x.getMaxIndexed() == y.getMaxIndexed() && x.vectorLength() > y.vectorLength())) { // this is important for correctness: we need to fully replicate the record with the smaller b.
//              // If both indexItems have the same maxIndexed value, we need to ensure that the shorter one is first, otherwise we lose results
//              // <<<<<<<<<<<<<<<<< THIS maxIndexed logic assumes that the tokens are sorted in DESCENDING order!!! Otherwise, the output is wrong
//              IndexItem tmp = x;
//              x = y;
//              y = tmp;
//            } 
//
//            outKey.set(x.getID(), y.getID());
//            payload.set(y.getID(), 1, y.vectorLength()); // the middle number is the initial overlap of 1
//            outValue.set(payload);
//            context.write(outKey, outValue);
//          }
//        }
//      } catch (Exception e) {
//        System.out.println(e); // Could not find any valid local directory for output/spill0.out
//      }
//    }
//
//  }
  
  public static class SimilarityMapperIndex extends Mapper<IntWritable, IndexItemArrayWritable, GenericKey, GenericValue> {

    private final GenericKey outKey = new GenericKey();
    private final GenericValue outValue = new GenericValue();
    private final HalfPair payload = new HalfPair();

    @Override
    public void map(IntWritable key, IndexItemArrayWritable value, Context context) throws IOException, InterruptedException {
      try {
        IndexItem[] postingList = value.toIndexItemArray();
        for (int i = 1; i < postingList.length; i++) { // NLJ to produce candidate pairs:
          for (int j = 0; j < i; j++) {
            IndexItem x = postingList[i];
            IndexItem y = postingList[j];

//            if (x.vectorLength() > y.vectorLength() || (x.vectorLength() == y.vectorLength() && x.getID() > y.getID())) { // erstes Vertauschkriterium für Korrektheit: Länge. Der kürzere muss vorne stehen, sonst verlieren wir Resultate. Falls sie gleich lang sind nehmen wir als Ordnung die ID.
            if ((x.getMaxIndexed() < y.getMaxIndexed()) || (x.getMaxIndexed() == y.getMaxIndexed() && x.vectorLength() > y.vectorLength())) { // this is important for correctness: we need to fully replicate the record with the smaller b.
              // If both indexItems have the same maxIndexed value, we need to ensure that the shorter one is first, otherwise we lose results
              // <<<<<<<<<<<<<<<<< THIS maxIndexed logic assumes that the tokens are sorted in DESCENDING order!!! Otherwise, the output is wrong
              IndexItem tmp = x;
              x = y;
              y = tmp;
            } 

            outKey.set(x.getID(), y.getID());
            payload.set(y.getID(), 1, y.vectorLength()); // the middle number is the initial overlap of 1
            outValue.set(payload);
            context.write(outKey, outValue);
          }
        }
      } catch (Exception e) {
        System.out.println(e); // Could not find any valid local directory for output/spill0.out
      }
    }

  }

  public static class SimilarityCombiner extends Reducer<GenericKey, GenericValue, GenericKey, GenericValue> {

    private final HalfPair payload = new HalfPair();
    private final GenericValue outValue = new GenericValue();

    @Override
    public void reduce(GenericKey key, Iterable<GenericValue> valuesIterable, Context context) throws IOException, InterruptedException {
      Iterator<GenericValue> values = valuesIterable.iterator();
      if (key.getSecondary() == -1) { // vector
        context.write(key, values.next());
      } else {
        int overlap = 0;
        HalfPair hp = null;
        while (values.hasNext()) {
          hp = (HalfPair) values.next().get();
          overlap += hp.getOverlap();
        }
        if (hp != null) {
          payload.set(hp.getID(), overlap, hp.getVectorSize());
          outValue.set(payload);
          context.write(key, outValue);
        }
      }
    }
  }
  
  public static class SimilarityReducer extends Reducer<GenericKey, GenericValue, Text, NullWritable> {
    private float threshold;
    private final Map<Integer, VectorComponentArrayWritable> pruned = new HashMap();
    private final Text outKey = new Text();
    private final NullWritable outValue = NullWritable.get();

    @Override
    public void reduce(GenericKey key, Iterable<GenericValue> valuesIterable, Context context) throws IOException, InterruptedException {
//      System.out.println("-----------------Neue Partition-----------------");
      Iterator<GenericValue> values = valuesIterable.iterator();
      long vectorID = key.getPrimary();
//      if (vectorID == 146730) {
//        System.out.println();
//      }
      try {
        // the vector is the first value
        VectorComponentArrayWritable vector = (VectorComponentArrayWritable) values.next().get();
        
        if (values.hasNext()) {

          HalfPair hp1 = (HalfPair) values.next().get(); // HalfPair: <termID, overlap, vectorSize>
          float overlap = hp1.getOverlap(); // wir zählen, wie oft das gleiche HalfPair vorkommt, denn das geht in die Berechnung der Similarity ein

          HalfPair hp2;
          while (values.hasNext()) {
            hp2 = (HalfPair) values.next().get();
            
            if (hp1.equals(hp2)) { // wenn die Term-ID identisch ist, summieren wir zum overlap etwas auf:
              overlap += hp2.getOverlap();
            } else {
              // output
              outputHelper(hp1, vectorID, vector, overlap, context);
              // start new stripe
              hp1 = hp2;
              overlap = hp1.getOverlap();
            }
          }
          // output the last one
          outputHelper(hp1, vectorID, vector, overlap, context);
        }
      } catch (java.lang.ClassCastException e) {
        // for some datasets and similarity thresholds, there is only a HalfPair without the record. (out_5000 and .5)
        System.out.println(e);
      }
    }

    private void outputHelper(HalfPair hp, long vectorID, VectorComponentArrayWritable vector, float overlap, Context context) throws IOException, InterruptedException {
      float similarity = 0;

      VectorComponentArrayWritable remainder = pruned.get(hp.getID());
      
      VectorComponent[] vectorArr = vector.toVectorComponentArray(); // das bleibt doch immer gleich...
      if (remainder != null) {  
        VectorComponent[] remainderArr = remainder.toVectorComponentArray();
        int i = 0;
        int j = 0;
        for (; i < remainderArr.length && j < vectorArr.length;) {
          long remainderCurrentTokenId = remainderArr[i].getID();
          long vectorCurrentTokenId = vectorArr[j].getID();
          if (remainderCurrentTokenId == vectorCurrentTokenId) {
            overlap++;
            i++;
            j++;
          } else if (remainderCurrentTokenId < vectorCurrentTokenId) { // <<<<<<<<<<<< ACHTUNG: Das nimmt absteigende Sortierung an, sonst kommt ein falscher Overlap raus!
            j++;
          } else {
            i++;
          }
        }
      } 

      float size0 = vectorArr.length;
      float size1 = hp.getVectorSize();
      float overlapFloat = overlap;

      similarity = overlapFloat / (size0 + size1 - overlapFloat);
      
      if (similarity > threshold) {
        outKey.set(vectorID + " " + hp.getID());
        context.write(outKey, outValue);
      }
    }

    @Override
    protected void setup(Context context) {
      threshold = context.getConfiguration().getFloat("theta", 0);
      try {
        Path pt = new Path(context.getConfiguration().get("indexPath"));
        Configuration conf = new Configuration(); // das "new Configuration()" kommt aus Beispielen und funktioniert
        FileSystem fs = FileSystem.get(conf);
        
        FileStatus[] file_status = fs.listStatus(pt); // das muss unbedingt ein Verzeichnis sein und kein Wildcard-Pfad!
        for (FileStatus fileStatus : file_status) {
          if (fileStatus.getPath().getName().contains("pruned")) {
            // Struktur: <LongWritable, VectorComponentArrayWritable>
            SequenceFile.Reader reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(fileStatus.getPath()));
//            long position = reader.getPosition();

            IntWritable key = new IntWritable();
            VectorComponentArrayWritable value = new VectorComponentArrayWritable();

            while(reader.next(key,value)) {
              pruned.put(key.get(), value);
              value = new VectorComponentArrayWritable(); // wrong results otherwise
            }
          }
        }
      } catch(Exception e) {
        System.out.println(e);
      }
    }
  }
}
