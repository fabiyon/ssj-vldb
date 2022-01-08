package de.huberlin.textualsimilarityhadoop;

import de.huberlin.vernicajoin.FuzzyJoinMemory;
import de.huberlin.vernicajoin.IntPairComparatorFirst;
import de.huberlin.vernicajoin.IntPairPartitionerFirst;
import de.huberlin.vernicajoin.IntPairWritable;
import de.huberlin.vernicajoin.ResultSelfJoin;
import de.huberlin.vernicajoin.VernicaElem;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.lib.InputSampler;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class PiJoinJob extends Configured implements Tool {
  private static final Logger log = Logger.getLogger(PiJoinJob.class);
  
  private static class LengthGroupAssignment {
    double expectedValue;
    int maxLength;
    public int[][] groupReplication;
    public int currentGroupID;
    
    public LengthGroupAssignment(double expectedValue, int maxLength, double theta) {
      this.expectedValue = expectedValue;
      this.maxLength = maxLength;
      
      int[] groupAssignment = new int[maxLength];
      int lastRawGroupID = 0;
      currentGroupID = 0;
      for (int i = 1; i < maxLength; i++) {
        int currentRawGroupID = (int)Math.floor((expectedValue / (double)i) * 20.0); // Gruppengröße erstmal 20. Diese Berechnung liefert nicht-dichte Gruppen-IDs, die wir hier gleich dicht machen:
        if (currentRawGroupID != lastRawGroupID) {
          currentGroupID++;
        }
        groupAssignment[i] = currentGroupID;
        lastRawGroupID = currentRawGroupID;
      }
      
      groupReplication = new int[maxLength][];
      for (int i = 1; i < maxLength; i++) {
        int upperBound = (int)Math.floor((double)i / theta);
        ArrayList<Integer> tmp = new ArrayList();
        for (int j = i; j <= upperBound && j < maxLength; j++) {
          if (!tmp.contains(groupAssignment[j])) {
            tmp.add(groupAssignment[j]);
          }
        }
        groupReplication[i] = new int[tmp.size()];
        for (int j = 0; j < tmp.size(); j++) {
          groupReplication[i][j] = tmp.get(j);
        }
      }
    }
  }
  
  public static class PivotMapper extends Mapper<LongWritable, Text, IntPairWritable, PiVal> {
    IntPairWritable outKey = new IntPairWritable();
    double eps;
//    GroupAssignment ga;
    int modulo;
    LengthGroupAssignment lga;
    int[] groupModulo;

    @Override
    protected void setup(Context context) {
//      pivPts = getPivotElements(context);
      eps = context.getConfiguration().getDouble("eps", 0);
      if (eps == 0.0) {
        log.error("Unable to get eps from configuration");
      }
      lga = new LengthGroupAssignment(context.getConfiguration().getDouble("expectedValue", 0), 100, eps);
//      ga = new GroupAssignment(1 - eps);
      modulo = context.getConfiguration().getInt("modulo", 5);
      String lengthStatisticsString[] = context.getConfiguration().get("lengthStatistics", "").split(",");
      int[] lengthStatistics = new int[lengthStatisticsString.length + 1]; // Key: Länge, Wert: reine eigene Gruppengröße (ohne Replikation)
      double numberOfSamples = context.getConfiguration().getInt("numberOfSamples", 1);
      for (int i = 1; i < lengthStatisticsString.length; i++) {
        lengthStatistics[i + 1] = (int)((Double.parseDouble(lengthStatisticsString[i]) / numberOfSamples) * 100000); // <<<< Anzahl der Records noch unbekannt
      }
      
      // Schätze die Gruppengrößen ab:
      int[] groupSizes = new int[lga.currentGroupID + 1];
      for (int l = 1; l < lengthStatistics.length; l++) { // iteriere über alle Längen und addiere die eigene Gruppengröße zu allen Gruppen, die die repliziert werden soll:
        for (int gid : lga.groupReplication[l]) {
          groupSizes[gid] += lengthStatistics[l];
        }
      }
      
      groupModulo = new int[lga.currentGroupID + 1]; // we don't use the index 0
      for (int gid = 1; gid <= lga.currentGroupID; gid++) {
        groupModulo[gid] = groupSizes[gid] / 50000; // << max. Partitionsgröße konfigurierbar machen
//        groupModulo[gid] = 0;
      }
      
//      System.out.println(Arrays.toString(groupModulo));
//      ga.setK(modulo);
    }

    @Override
    public void map(LongWritable inputKey, Text inputValue, Context context) throws IOException, InterruptedException {
      PiVal piVal = new PiVal(inputValue.toString(), 0); // pivPts.size()
      
//      int modulo = 4; // => ergibt 6 Partitionen, 3 Self-Join-Partitionen, 3 RxS-Partitionen
      int mainGroup = piVal.rid % modulo;
      
      for (int i = 0; i < modulo; i++) {
        for (int j = i; j < modulo; j++) {
          int gid = i + (modulo - 1) * j;
          if (mainGroup == i && mainGroup == j) { // Self-Join-Gruppe:
            outKey.set(gid, 0); // 0 ist das Flag für Self-Join
            context.write(outKey, piVal);
          } else if (mainGroup == i) {
            outKey.set(gid, 1); // 1 ist das Flag für die linke Partition eines RxS-Join
            context.write(outKey, piVal);
          } else if (mainGroup == j) {
            outKey.set(gid, 2); // 2 ist das Flag für die rechte Partition eines RxS-Join
            context.write(outKey, piVal);
          }
        }
      }
      
//      int mod = 10;
//      int[] signatur = new int[10];
//      for (int i = 0; i < piVal.tokens.length; i++) {
//        signatur[piVal.tokens[i] % mod] = 1;
//      }
//      
//      System.out.println(Arrays.toString(signatur));
      
      // Partitionierung und Replikation nach Länge + Theta, aber noch mit vielen Duplikaten:
//      int[] groups = lga.groupReplication[piVal.tokens.length];
//      for (int gid : groups) {
//        if (this.groupModulo[gid] == 0) { // keine weitere Partitionierung notwendig
//          outKey.set(gid);
//          context.write(outKey, piVal);
//        } else { // die voraussichtliche Partitionsgröße der Gruppe ist zu groß, daher weitere Theta-Partitionierung:
//          int currentModulo = piVal.rid % (this.groupModulo[gid] + 1);
//          // output rows:
//          int row = 0;
//          int col = currentModulo;
//          for (; row <= currentModulo; row++) {
//            outKey.set(gid*1000000 + 1000*row + col);
//            context.write(outKey, piVal);
//          }
//
//          // output cols:
//          row = currentModulo;
//          col++; // otherwise we replicate too much
//          for (; col <= this.groupModulo[gid]; col++) {
//            outKey.set(gid*1000000 + 1000*row + col);
//            context.write(outKey, piVal);
//          }
//        }
//      }
    }
  }

  public static class JoinReducer extends Reducer<IntPairWritable, PiVal, NullWritable, Text> {
    NullWritable outKey = NullWritable.get();
    double eps;
  
    @Override
    public void setup(Context context) {
      eps = context.getConfiguration().getDouble("eps", 0);
      if (eps == 0.0) {
        log.error("Unable to get eps from configuration");
      }
    }
    
    @Override
    public void reduce(IntPairWritable key, Iterable<PiVal> values, Context context) throws IOException, InterruptedException {
      Iterator<PiVal> i = values.iterator();
      ArrayList<PiVal> buffer = new ArrayList();
      PiVal currentVal;
      if (key.getSecond() == 0) { // Self-Join:
        while (i.hasNext()) {
          currentVal = i.next();
          for (PiVal compareVal : buffer) {
            if (compareVal.getJaccardDistanceBetween(currentVal) < eps) {
              context.write(outKey, new Text(compareVal.rid + " " + currentVal.rid));
            }
          }
          buffer.add(new PiVal(currentVal));
        }
      } else { // RxS-Join:
        while (i.hasNext()) {
          currentVal = i.next();
          if (key.getSecond() < 2) { // Linke Partition kommt nur in den Puffer
            buffer.add(new PiVal(currentVal));
          } else { // Rechte Partition wird gejoint
            for (PiVal compareVal : buffer) {
              if (compareVal.getJaccardDistanceBetween(currentVal) < eps) {
                context.write(outKey, new Text(compareVal.rid + " " + currentVal.rid));
              }
            }
          }
        }
      }
      
      
//      PPJoinPlus pp = new PPJoinPlus((float)eps);
//      Iterator<PiVal> i = values.iterator();
//      ArrayList<Long> results = new ArrayList();
//      ArrayList<PiVal> buffer = new ArrayList();
//      PiVal currentVal, compareVal;
////      double currentLower;
//      Iterator<PiVal> bufferItr;
      
//      HashMap<Integer, Integer> rids = new HashMap();
//      FuzzyJoinMemory fuzzyJoinMemory = new FuzzyJoinMemory((float)eps);
//      int crtRecordId = 0;
//
//      
//      while (i.hasNext()) {
////        anzahlRecords++;
//        currentVal = i.next();
//        int[] record = currentVal.tokens;
//        rids.put(crtRecordId, currentVal.rid);
//        crtRecordId++;
//        ArrayList<ResultSelfJoin> results = fuzzyJoinMemory
//                .selfJoinAndAddRecord(record);
//        for (ResultSelfJoin result : results) {
//          int rid1 = rids.get(result.indexX);
//          int rid2 = rids.get(result.indexY);
//          if (rid1 < rid2) {
//            int rid = rid1;
//            rid1 = rid2;
//            rid2 = rid;
//          }
//
//          context.write(outKey, new Text(rid1 + " " + rid2));
//        }

        
        
        
//        pp.selfJoinAndAddRecord(currentVal.tokens, currentVal.rid, results);
//        for (Long res : results) {
//          context.write(outKey, new Text(currentVal.rid + " " + res));
//        }
        
//        bufferItr = buffer.iterator();
//        while (bufferItr.hasNext()) { // iteriere über alle vorher gesehenen RIDs
////          currentLower = 0;
//          compareVal = bufferItr.next();
////          for (int j = 0; j < currentVal.pivotDistance.length; j++) { //iteriere über alle Pivot-Abstände in der Tabelle:
////            currentLower = Math.max(currentLower, Math.abs(compareVal.pivotDistance[j] - currentVal.pivotDistance[j]));
////            if (currentLower > eps) { // kleine Optimierung: wir können aufhören
////              break;
////            }
////          }
////          if (currentLower < eps) { // das ist ein Kandidat:
//////            anzahlKandidaten++;
////          if ((compareVal.rid == 1559 && currentVal.rid == 2614) || (compareVal.rid == 2614 && currentVal.rid == 1559)) {
////            System.out.println();
////          }
//            if (compareVal.getJaccardDistanceBetween(currentVal) < eps) {
//              context.write(outKey, new Text(compareVal.rid + " " + currentVal.rid));
//            } 
////          }
//        }
//        buffer.add(new PiVal(currentVal)); // klonen ist unbedingt nötig, da Hadoop beim Iterieren die Werte immer neu schreibt
//      }
    }
  }

  // Join nicht ausführen, sondern nur dessen Kosten abschätzen
  public static class StatisticsReducer extends Reducer<IntPairWritable, PiVal, NullWritable, Text> {
    NullWritable outKey = NullWritable.get();
    double eps;
  
    @Override
    public void setup(Context context) {
      eps = context.getConfiguration().getDouble("eps", 0);
      if (eps == 0.0) {
        log.error("Unable to get eps from configuration");
      }
    }
    
    @Override
    public void reduce(IntPairWritable key, Iterable<PiVal> values, Context context) throws IOException, InterruptedException {
      if (key.getSecond() == 0) { // Self-Join:
        int anzahlRecords = 0; // nur interessehalber für die Statistik
        Iterator<PiVal> i = values.iterator();
        PiVal currentVal;
        int kuerzesterRecord = 999;
        int laengsterRecord = 0;
        double durchschnitt = 0.0;

        while (i.hasNext()) {
          anzahlRecords++;
          currentVal = i.next();
          if (currentVal.tokens.length > laengsterRecord) {
            laengsterRecord = currentVal.tokens.length;
          }
          if (currentVal.tokens.length < kuerzesterRecord) {
            kuerzesterRecord = currentVal.tokens.length;
          }
          durchschnitt += currentVal.tokens.length;
        }
        context.write(outKey, new Text(key.toString() + ": #R=" + anzahlRecords + ", L={" + kuerzesterRecord + ".." + laengsterRecord + "}, D=" + (durchschnitt / (double) anzahlRecords)));
      } else {
        int anzahlRecordsR = 0;
        int anzahlRecordsS = 0;
        Iterator<PiVal> i = values.iterator();
        PiVal currentVal;
        int kuerzesterRecord = 999;
        int laengsterRecord = 0;
        double durchschnitt = 0.0;

        while (i.hasNext()) {
          currentVal = i.next();
          if (key.getSecond() == 1) {
            anzahlRecordsR++;
          } else {
            anzahlRecordsS++;
          }
          if (currentVal.tokens.length > laengsterRecord) {
            laengsterRecord = currentVal.tokens.length;
          }
          if (currentVal.tokens.length < kuerzesterRecord) {
            kuerzesterRecord = currentVal.tokens.length;
          }
          durchschnitt += currentVal.tokens.length;
        }
        context.write(outKey, new Text(key.toString() + ": #R=" + anzahlRecordsR + ", #S=" + anzahlRecordsS + ", L={" + kuerzesterRecord + ".." + laengsterRecord + "}, D=" + (durchschnitt / (double) (anzahlRecordsR + anzahlRecordsS))));
      }
      
    }
  }

  
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new PiJoinJob(), args);
  }

  @Override
  public int run(String[] args) throws Exception {
    System.out.println("PiJoin");
    log.setLevel(Level.OFF);
    ParameterParser pp = new ParameterParser(args);
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);
    
    String inputDir = pp.getInput() + "/";
    String outputDir = pp.getOutput() + "/";
    String pivDir = outputDir + "PIVOTS/";
    Path pivPath = new Path(pivDir + "/pivots.txt");
    Path inputPath = new Path(inputDir);
    conf.setDouble("eps", 1 - new Double(pp.getTheta()));
    conf.setInt("modulo", pp.getModulo());
    Path outputPath = new Path(outputDir + "/OUTPUT/");
    fs.delete(outputPath, true);
    
    int[] lengthStatistics = new int[100]; // we assume a max. Length of 100
    Object[] pivots = getPivots(inputPath, pp.getNumberOfPivots()); //generate pivot points from inputPath directory
//    FSDataOutputStream out = fs.create(pivPath); //write pivots to file      
    for (Object pivot : pivots) {
      int lineLength = StringUtils.countMatches(((Text) pivot).toString(), ",") + 1;
      lengthStatistics[lineLength] += 1;
//      splitAndWrite(((Text) pivot).toString() + '\n', out);
    }
    double expectedValue = 0.0;
    String lengthStatisticsString = "";
    for (int i = 1; i < lengthStatistics.length; i++) {
      if (!lengthStatisticsString.isEmpty()) {
        lengthStatisticsString += ",";
      }
      expectedValue += i * lengthStatistics[i];
      lengthStatisticsString += lengthStatistics[i];
    }
    expectedValue /= (double)pp.getNumberOfPivots();
    
    System.out.println(expectedValue);
    conf.setDouble("expectedValue", expectedValue);
    conf.setStrings("lengthStatistics", lengthStatisticsString);
    conf.setInt("numberOfSamples", pp.getNumberOfPivots());
//    out.close();
    conf.set("mapreduce.input.fileinputformat.split.maxsize", "3000000"); // number of bytes

    Job job = new Job(conf, "PiJoin");
    job.setJarByClass(PiJoinJob.class);
    
    job.setMapperClass(PivotMapper.class);
    job.setMapOutputKeyClass(IntPairWritable.class);
    job.setMapOutputValueClass(PiVal.class);
    job.setPartitionerClass(IntPairPartitionerFirst.class);
    job.setGroupingComparatorClass(IntPairComparatorFirst.class);
//    conf.set("mapreduce.map.output.compress", "true"); // brachte keine Besserung bei vielen Pivots

    job.setReducerClass(JoinReducer.class); // StatisticsReducer
    // job.setNumReduceTasks(numReduces);
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(Text.class);

    //add file to cache
//    String cacheFileName = pivPath.toString() + "#pivots";
//    job.addCacheFile(new URI(cacheFileName));
    
    job.setInputFormatClass(TextInputFormat.class);
    // specify input and output DIRECTORIES (not files)
    FileInputFormat.addInputPath(job, inputPath);
    FileOutputFormat.setOutputPath(job, outputPath);

    job.waitForCompletion(true);
    return 0;
  }

  Object[] getPivots(Path input, int numPivs) throws IOException {
    JobConf job = new JobConf();
    job.setInputFormat(KeyValueTextInputFormat.class);
    job.setMapOutputKeyClass(Text.class);
    org.apache.hadoop.mapred.FileInputFormat.addInputPath(job, input);
    final KeyValueTextInputFormat inf = (KeyValueTextInputFormat) job.getInputFormat();
    InputSampler.Sampler<Text, Text> sampler = new InputSampler.RandomSampler(1.0, numPivs, 100); // frequency, number, max splits sampled
    Object[] samples = sampler.getSample(inf, job);
    return samples;
  }

}
