package de.huberlin.mgjoin;

import de.huberlin.textualsimilarityhadoop.ParameterParser;
import edu.uci.ics.fuzzyjoin.similarity.SimilarityFiltersJaccard;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.TreeMap;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


/**
 *
 * @author fabi
 */
public class MGBalanceJob extends Configured implements Tool {
  
  @Override
  public int run(String[] args) throws Exception {
    ParameterParser pp = new ParameterParser(args);
    
    Configuration conf = this.getConf();
    FileSystem filesystem = FileSystem.get(getConf());
    String inputPathString = pp.getInput();
    Path inputPath = new Path(inputPathString);

//    conf.set("tokenFrequencyPath", pp.getOutput() + MGJoinDriver.TOKENSWITHFREQUENCIES_PATH_SUFFIX);
    
    Path balancedPath = new Path(pp.getOutput() + MGJoinDriver.BALANCED_PATH_SUFFIX);
    filesystem.delete(balancedPath, true);
    
    Path outputPath = new Path(pp.getOutput());
    filesystem.delete(outputPath, true);

    // 2. Step: Balance according to size:
    System.out.println("MG Step 2: Distribute by length");
    Job job2 = Job.getInstance(conf, "Step 2: Distribute by length");
    job2.setJarByClass(MGBalanceJob.class);
    
    job2.setMapperClass(BalanceMapper.class);
    job2.setMapOutputKeyClass(NullWritable.class);
    job2.setMapOutputValueClass(Text.class);
    
    if (job2.getNumReduceTasks() < 2) {
      job2.setNumReduceTasks(2); // this needs to be set in local environment to trigger the non-default partitioner etc. 
    }
    job2.setPartitionerClass(MGPartitioner.class); // this is only called if there is more than 1 reducer configured! It does not require a Reducer.
    
    FileInputFormat.addInputPath(job2, inputPath);
    FileOutputFormat.setOutputPath(job2, balancedPath); 
    job2.waitForCompletion(true);
    
    return 0;
  }
  
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new Configuration(), new MGBalanceJob(), args);
  }

  
  
  public static class MGPartitioner extends Partitioner<NullWritable, Text> implements Configurable {
    private Configuration configuration;

    @Override
    public int getPartition(NullWritable key, Text value, int numPartitions) {
      int count = StringUtils.countMatches(value.toString(), ",") + 1;
      return (int)(count % numPartitions); // the records are partitioned round robin by their length
    }

    @Override
    public void setConf(Configuration conf) {
      this.configuration = conf;
    }

    @Override
    public Configuration getConf() {
      return configuration;
    }
  } // end of StripePartitioner
  
  
  public static class BalanceMapper extends Mapper<Object, Text, NullWritable, Text> {
    private final NullWritable outKey = NullWritable.get();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      context.write(outKey, value);
    }
  }
  
  
}
