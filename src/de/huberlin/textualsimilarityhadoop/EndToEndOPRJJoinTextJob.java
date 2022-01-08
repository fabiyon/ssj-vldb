package de.huberlin.textualsimilarityhadoop;

import de.huberlin.textualsimilarityhadoop.EndToEndTextJoiner.EndReducer2;
import de.huberlin.textualsimilarityhadoop.EndToEndTextOPRJJoiner.OPRJEndMapper1;
import de.huberlin.textualsimilarityhadoop.EndToEndTextOPRJJoiner.OPRJEndReducer1;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


/**
 *
 * @author fabi
 * Implements the (more efficient) OPRJ
 */
public class EndToEndOPRJJoinTextJob extends Configured implements Tool {
  
  @Override
  public int run(String[] args) throws Exception {
    System.out.println("End-to-end: Join text to the rid pairs");
    ParameterParser pp = new ParameterParser(args);
    String dataTypeString = pp.getDataType();
    
    Configuration conf = this.getConf();
    FileSystem filesystem = FileSystem.get(getConf());

    Path inputPath;
    if (dataTypeString.equals(DetectInputDataType.DataType.RAW.toString())) {
      inputPath = new Path(pp.getOutput() + "Tokenized");
    } else { // String + Integer:
      inputPath = new Path(pp.getInput());
    }
    
    Path outputPath = new Path(pp.getOutput());
    Path intermediateOutputPath = new Path(pp.getOutput() + "Intermediate");
    filesystem.delete(intermediateOutputPath, true);
    filesystem.rename(outputPath, intermediateOutputPath);              // move previous "final" output to ..."Intermediate"
    
    conf.set("ridPairs", intermediateOutputPath.toString()); // intermediate file(s) for the subsequent step

    Job job4a = Job.getInstance(conf, "Join text in one MR step");
    job4a.setJarByClass(EndToEndTextOPRJJoiner.class);

    job4a.setMapperClass(OPRJEndMapper1.class);
    job4a.setMapOutputKeyClass(LongWritable.class);
    job4a.setMapOutputValueClass(Text.class);

    job4a.setReducerClass(OPRJEndReducer1.class);
    job4a.setOutputKeyClass(Text.class);
    job4a.setOutputValueClass(NullWritable.class);

    FileInputFormat.addInputPath(job4a, inputPath);   
    FileOutputFormat.setOutputPath(job4a, outputPath);

    job4a.waitForCompletion(true);
    // delete intermediate files:
    filesystem.delete(intermediateOutputPath, true);
    if (dataTypeString.equals(DetectInputDataType.DataType.RAW.toString())) {
      filesystem.delete(inputPath, true); // this is in fact an intermediate result in this case
    }
    
    return 0;
  }
  
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new Configuration(), new EndToEndOPRJJoinTextJob(), args);
  }
}
