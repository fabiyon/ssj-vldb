package de.huberlin.textualsimilarityhadoop;

import de.huberlin.textualsimilarityhadoop.EndToEndTextJoiner.EndMapper1;
import de.huberlin.textualsimilarityhadoop.EndToEndTextJoiner.EndMapper2;
import de.huberlin.textualsimilarityhadoop.EndToEndTextJoiner.EndReducer1;
import de.huberlin.textualsimilarityhadoop.EndToEndTextJoiner.EndReducer2;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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
 */
public class EndToEndJoinTextJob extends Configured implements Tool {
  
  @Override
  public int run(String[] args) throws Exception {
    System.out.println("End-to-end: Join text to the rid pairs");
    ParameterParser pp = new ParameterParser(args);
    
    Configuration conf = this.getConf();
    FileSystem filesystem = FileSystem.get(getConf());

    Path inputPath = new Path(pp.getOutput() + "Tokenized");
    Path outputPath = new Path(pp.getOutput());
    Path intermediateOutputPath = new Path(pp.getOutput() + "Intermediate");
    filesystem.delete(intermediateOutputPath, true);
    filesystem.rename(outputPath, intermediateOutputPath);              // move previous "final" output to ..."Intermediate"
    
    Path intermediateOutputPath1 = new Path(pp.getOutput() + "end1");   // intermediate path for this join
    filesystem.delete(intermediateOutputPath1, true);
    

    Job job4a = Job.getInstance(conf, "Step 4a: Join text");
    job4a.setJarByClass(EndToEndTextJoiner.class);

    job4a.setMapperClass(EndMapper1.class);
    job4a.setMapOutputKeyClass(IntWritable.class);
    job4a.setMapOutputValueClass(Text.class);

    job4a.setReducerClass(EndReducer1.class);
    job4a.setOutputKeyClass(Text.class);
    job4a.setOutputValueClass(NullWritable.class);

    MultipleInputs.addInputPath(job4a, intermediateOutputPath, TextInputFormat.class);
    MultipleInputs.addInputPath(job4a, inputPath, TextInputFormat.class);

    
    FileOutputFormat.setOutputPath(job4a, intermediateOutputPath1);

    job4a.waitForCompletion(true);

    Job job4b = Job.getInstance(conf, "Step 4b: Join text");
    job4b.setJarByClass(EndToEndTokenizer.class);

    job4b.setMapperClass(EndMapper2.class);
    job4b.setMapOutputKeyClass(Text.class);
    job4b.setMapOutputValueClass(Text.class);

    job4b.setReducerClass(EndReducer2.class);
    job4b.setOutputKeyClass(Text.class);
    job4b.setOutputValueClass(NullWritable.class);

    FileInputFormat.addInputPath(job4b, intermediateOutputPath1);
    FileOutputFormat.setOutputPath(job4b, outputPath);

    job4b.waitForCompletion(true);

    // delete intermediate files:
    filesystem.delete(intermediateOutputPath1, true);
    filesystem.delete(intermediateOutputPath, true);
    filesystem.delete(inputPath, true); // this is in fact an intermediate result in this case
    
    return 0;
  }
  
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new Configuration(), new EndToEndJoinTextJob(), args);
  }
}
