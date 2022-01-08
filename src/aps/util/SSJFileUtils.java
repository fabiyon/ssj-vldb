package aps.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.ReflectionUtils;

import aps.io.VectorComponentArrayWritable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;

public final class SSJFileUtils {

  private static final Log LOG = LogFactory.getLog(SSJFileUtils.class);

  static long fixMapFile(FileSystem fs, Path dir, Class<? extends Writable> keyClass,
          Class<? extends Writable> valueClass, boolean dryrun, Configuration conf) throws Exception {
    String dr = (dryrun ? "[DRY RUN ] " : "");
    Path data = new Path(dir, MapFile.DATA_FILE_NAME);
    Path index = new Path(dir, MapFile.INDEX_FILE_NAME);
    int indexInterval = 128;
    indexInterval = conf.getInt("io.map.index.interval", indexInterval);
    if (!fs.exists(data)) {
      // there's nothing we can do to fix this!
      throw new Exception(dr + "Missing data file in " + dir + ", impossible to fix this.");
    }
    if (fs.exists(index)) {
      // no fixing needed
      return -1;
    }
    SequenceFile.Reader dataReader = new SequenceFile.Reader(fs, data, conf);
    if (!dataReader.getKeyClass().equals(keyClass)) {
      throw new Exception(dr + "Wrong key class in " + dir + ", expected" + keyClass.getName() + ", got "
              + dataReader.getKeyClass().getName());
    }
    if (!dataReader.getValueClass().equals(valueClass)) {
      throw new Exception(dr + "Wrong value class in " + dir + ", expected" + valueClass.getName() + ", got "
              + dataReader.getValueClass().getName());
    }
    long cnt = 0L;
    Writable key = ReflectionUtils.newInstance(keyClass, conf);
    Writable value = ReflectionUtils.newInstance(valueClass, conf);
    SequenceFile.Writer indexWriter = null;
    if (!dryrun) {
      indexWriter = SequenceFile.createWriter(fs, conf, index, keyClass, LongWritable.class);
    }
    try {
      long pos = 0L;
      LongWritable position = new LongWritable();
      while (dataReader.next(key, value)) {
        cnt++;
        if (cnt % indexInterval == 0) {
          position.set(pos);
          if (!dryrun) {
            indexWriter.append(key, position);
          }
        }
        pos = dataReader.getPosition();
      }
    } catch (Throwable t) {
      // truncated data file. swallow it.
    }
    dataReader.close();
    if (!dryrun) {
      indexWriter.close();
    }
    return cnt;
  }

  public static boolean readRestFile(Configuration conf, Map<? super Integer, ? super VectorComponentArrayWritable> result) {
    String fname = null;
    // open the first file in the DistributedCache
    try {
      MapFile.Reader reader = getLocalMapFileReader(conf);
      IntWritable key = new IntWritable();
      VectorComponentArrayWritable value = new VectorComponentArrayWritable();
      while (reader.next(key, value)) {
        result.put(Integer.valueOf(key.get()), new VectorComponentArrayWritable(value));
      }
      return true;
    } catch (IOException e) {
      e.printStackTrace();
    }
    return false;
  }

  public static boolean readRestFile(Reducer.Context context, Map<? super Integer, ? super VectorComponentArrayWritable> result, int from, int to) {
    Configuration conf = context.getConfiguration();
    String fname = null;
    // open the first file in the DistributedCache
    try {
      MapFile.Reader reader = getRemoteMapFileReader(context); // <<<<<<<<<<<<<<<< Das hier liefert was leeres aufm Cluster!
      IntWritable key = new IntWritable(from);
      VectorComponentArrayWritable value = new VectorComponentArrayWritable();
      key = (IntWritable) reader.getClosest(key, value);
      LOG.info("First read key: " + key);
      result.put(Integer.valueOf(key.get()), new VectorComponentArrayWritable(value));
      while (reader.next(key, value) && key.get() < to) {
        result.put(Integer.valueOf(key.get()), new VectorComponentArrayWritable(value));
      }
      return true;
    } catch (IOException e) {
      e.printStackTrace();
    }
    return false;
  }

  /**
   * Merges the various sequence files containing the rest (pruned parts) and
   * create an index for them
   *
   * @param conf
   * @param indexPath
   * @param dirName
   * @param indexInterval
   * @throws java.io.IOException
   */
  public static void mergeRestFile(Job job, Path indexPath, String dirName, int indexInterval) throws IOException {
    /*
    So funktionierts beim Ball-Hashing 1:
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
    */
    
    
    System.out.println("======= mergeRestFile()");
    Configuration conf = job.getConfiguration();
    FileSystem fs = FileSystem.get(conf);
    Path pruned = new Path(indexPath, dirName);
    FileStatus parts[] = fs.globStatus(new Path(pruned + "*")); // hier gibts bei der lokalen Ausführung ein Problem, wenn da ein HDFS-Pfad steht...
    List<Path> pruned_parts = new ArrayList(parts.length);
    for (FileStatus partFile : parts) {
      if (partFile.getLen() > 0) {
        pruned_parts.add(partFile.getPath());
      }
    }
    LOG.info(String.format("Found %d unindexed parts, merging %d non empty files", parts.length, pruned_parts
            .size()));
    if (pruned_parts.size() > 0) {
      System.out.println("======= sorter started");
      SequenceFile.Sorter sorter = new SequenceFile.Sorter(fs, new IntWritable.Comparator(), IntWritable.class,
              VectorComponentArrayWritable.class, conf); // könnte es sein, dass der auf der lokalen Datei arbeitet und eben nicht die, die auf dem HDFS liegt?
      
//      fs.mkdirs(pruned);
//      Path pruneOutPath = new Path("hdfs://dbis41:9000/tmp/testdata"); // new Path(pruned.toString().replace("hdfs:/user", "hdfs://dbis41:9000/user"), "data"); // dieser Dateipfad dient als Ausgabe für sorter.merge. Er darf noch _nicht_ existieren.
      // Es liegt _nicht_ daran, dass das ein HDFS-Pfad ist. Wenn man den pruneOutPath zu einem lokalen file-Pfad macht, kommt eine java.lang.IllegalArgumentException: Wrong FS: file:/home/fier/data, expected: hdfs://dbis41:9000
      // Der Fehler bleibt, wenn man new Path("hdfs://dbis41:9000/user/fier/testdata") verwendet: fs.LocalDirAllocator$AllocatorPerContext: Disk Error Exception: org.apache.hadoop.util.DiskChecker$DiskErrorException: Cannot create directory: /user/fier
      // auch ein ganz anderer Pfad geht nicht, gleiche Fehlermeldung: Path("hdfs://dbis41:9000/ssjtmp/testdata")
      // spannend: org.apache.hadoop.util.DiskChecker$DiskErrorException: Directory is not writable: / -> Das heißt, er versucht den HDFS-Pfad 1:1 auf die lokale Platte zu projezieren. Dann müsste /tmp gehen!
      // wirrerweise legt er diese Datei dann aber nicht im lokalen /tmp an, sondern im HDFS-tmp. Auch recht...
      
//      System.out.println("==========mergeRestFile START");
//      for (int i = 0; i < pruned_parts.size(); i++) {
//        System.out.println("prunedPart[" + i + "]: " + pruned_parts.get(i));
//      }
//      System.out.println("pruneOutPath: " + pruneOutPath);
//      System.out.println("==========mergeRestFile END");
      
      // merge(Path[] inFiles, Path outFile): Merge the provided files.
//      sorter.merge(pruned_parts.toArray(new Path[pruned_parts.size()]), pruneOutPath);
      
      // verschiebe die testdata-Datei nach pruned/data:
//      fs.rename(pruneOutPath, new Path(pruned, "data")); // FUNKTIONIERT!!! :-)))
      
      Path pruneOutPath = new Path(pruned + "/data");
      sorter.merge(pruned_parts.toArray(new Path[pruned_parts.size()]), pruneOutPath);
      
      try {
        MapFile.Writer.setIndexInterval(conf, indexInterval);
        long numberOfEntries = SSJFileUtils.fixMapFile(fs, pruned, IntWritable.class,
                VectorComponentArrayWritable.class, false, conf); // <<<<<<<<<<<<<<<<<<<<<<<<<< vermutlich funktioniert das hier nicht auf dem Cluster. 
        // Die Pruned-Records werden zwar rausgeschrieben, aber nicht sortiert.
        // allerdings sortiert diese Methode auch nicht, sondern partitioniert
        System.out.println(String.format("========= Number of pruned entries: %d", numberOfEntries));
        LOG.info(String.format("Number of pruned entries: %d", numberOfEntries));
      } catch (Exception e) {
        e.printStackTrace();
      }
      LOG.info(String.format("Adding %s to the DistributedCache", pruned.getName()));
//      pruned.suffix("#pruned");
      URI outPrunedUri;
      try {
        outPrunedUri = new URI(pruned.toUri().toString()); //  + "#pruned"
        job.addCacheFile(outPrunedUri);
      } catch (URISyntaxException ex) {
        Logger.getLogger(SSJFileUtils.class.getName()).log(Level.SEVERE, null, ex);
      }
      
//      DistributedCache.addCacheFile(pruned.toUri(), conf); // hier wird der verteilte (nicht-lokale) Cache verwendet.
    }
    for (FileStatus p : parts) {
      fs.delete(p.getPath(), true);
    }
  }

  /**
   * Opens the first MapFile in the DistributedCache
   *
   * @param conf configuration for the job
   * @return the reader for the opened MapFile
   * @throws IOException
   */
  public static MapFile.Reader getLocalMapFileReader(Configuration conf) throws IOException {
    // open the first  in the DistributedCache
    
    // throws NullPointerException: <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
//    URI[] uris = DistributedCache.getCacheFiles(conf); // lokal z. B. /home/fabi/researchProjects/textualSimilarity/data/mrsimjoin/input-index-0.6-s1_20150421-171851/pruned
//    if (uris != null || uris.length > 0 && uris[0] != null) {
//      FileSystem fs = FileSystem.get(uris[0], conf);
//      return new MapFile.Reader(fs, uris[0].getPath(), conf);
//    } else {
//      throw new IOException("Could not read lexicon file in DistributedCache");
//    }

    // ALT (lokal):
		Path[] localFiles = DistributedCache.getLocalCacheFiles(conf);
		if (localFiles != null && localFiles.length > 0 && localFiles[0] != null) {
			String mapFileDir = localFiles[0].toString();
			FileSystem fs = FileSystem.getLocal(conf);
			return new MapFile.Reader(fs, mapFileDir, conf);
		} else {
			throw new IOException("Could not read lexicon file in DistributedCache"); // auf dem Cluster wird bei Aufruf des letzten Reduce diese IOException geworfen,
                        // d. h. localFiles war leer.
		}
  }
  
  // neu von mir:
  public static MapFile.Reader getRemoteMapFileReader(Reducer.Context context) throws IOException {
    URI[] uris = context.getCacheFiles();
    if (uris != null || uris.length > 0 && uris[0] != null) {
      URI test = uris[0].normalize();
      Path testPath = new Path(test);
      System.out.println("========= getRemoteMapFileReader: " + testPath.toString());
      return new MapFile.Reader(testPath, context.getConfiguration()); // new Path("./pruned")
    } else {
      throw new IOException("Could not read lexicon file in DistributedCache");
    }
    
//    Configuration conf = context.getConfiguration();
//    URI[] uris = DistributedCache.getCacheFiles(conf); // lokal z. B. /home/fabi/researchProjects/textualSimilarity/data/mrsimjoin/input-index-0.6-s1_20150421-171851/pruned
//    if (uris != null || uris.length > 0 && uris[0] != null) {
//      FileSystem fs = FileSystem.get(uris[0], conf);
//      return new MapFile.Reader(fs, uris[0].getPath(), conf);
//    } else {
//      throw new IOException("Could not read lexicon file in DistributedCache");
//    }
  }
}
