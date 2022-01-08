package de.huberlin.textualsimilarityhadoop;

import de.huberlin.textualsimilarityhadoop.EndToEndTokenizer.TokenizeAssignMapper;
import de.huberlin.textualsimilarityhadoop.EndToEndTokenizer.TokenizeAssignRidMapper;
import de.huberlin.textualsimilarityhadoop.EndToEndTokenizer.TokenizeMapper;
import de.huberlin.textualsimilarityhadoop.EndToEndTokenizer.TokenizeReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


/**
 *
 * @author fabi
 */
public class EndToEndTokenizeJob extends Configured implements Tool {
  
  @Override
  public int run(String[] args) throws Exception {
    System.out.println("End-to-end Tokenizer");
    ParameterParser pp = new ParameterParser(args);
    
    Configuration conf = this.getConf();
    conf.set("tokenizerChoice", "FS");
    conf.set("increaseFactor", "1");
    FileSystem filesystem = FileSystem.get(getConf());
    
    String inputString = pp.getInput();               // INPUT
    Path inputPath = new Path(inputString);
    
    String rawWithRidString = pp.getOutput() + "RawWithRid";   // PUTPUT 1 
    Path rawWithRidPath = new Path(rawWithRidString);
    filesystem.delete(rawWithRidPath, true);
    
    String tokenString = pp.getOutput() + "Tokens";   // PUTPUT 2
    Path tokenStringPath = new Path(tokenString);
    filesystem.delete(tokenStringPath, true);
    
    String newInput = pp.getOutput() + "Tokenized";   // OUTPUT
    Path newInputPath = new Path(newInput);
    filesystem.delete(newInputPath, true);

    conf.set("mapreduce.input.fileinputformat.split.minsize", "" + Long.MAX_VALUE); // causes Hadoop to only use one Map
    System.out.println("Step -1: assign Record IDs in a Map-Only Job");
    Job job1 = Job.getInstance(conf, "Step -1: assign Record IDs in a Map-Only Job");
    job1.setJarByClass(EndToEndTokenizer.class);

    job1.setMapperClass(TokenizeAssignRidMapper.class);
    job1.setMapOutputKeyClass(Text.class);
    job1.setMapOutputValueClass(NullWritable.class);
    job1.setNumReduceTasks(0);

    FileInputFormat.addInputPath(job1, inputPath);
    FileOutputFormat.setOutputPath(job1, rawWithRidPath);

    job1.waitForCompletion(true);
    
    conf.unset("mapreduce.input.fileinputformat.split.maxsize");
    System.out.println("Step 0a: Tokenize");
    Job job0a = Job.getInstance(conf, "Step 0a: Tokenize");
    job0a.setJarByClass(EndToEndTokenizer.class);

    job0a.setMapperClass(TokenizeMapper.class);
    job0a.setMapOutputKeyClass(Text.class);
    job0a.setMapOutputValueClass(NullWritable.class);

    job0a.setReducerClass(TokenizeReducer.class);
    job0a.setCombinerClass(TokenizeReducer.class);
    job0a.setOutputKeyClass(Text.class);
    job0a.setOutputValueClass(NullWritable.class);

    FileInputFormat.addInputPath(job0a, inputPath);
    FileOutputFormat.setOutputPath(job0a, tokenStringPath);

    job0a.waitForCompletion(true);

    conf.set("tokenString", tokenString); // intermediate file(s) for the subsequent step
    System.out.println("Step 0b: Write tokenized records");
    Job job0b = Job.getInstance(conf, "Step 0b: Write tokenized records");
    job0b.setJarByClass(EndToEndTokenizer.class);

    job0b.setMapperClass(TokenizeAssignMapper.class);
    job0b.setMapOutputKeyClass(Text.class); // Format: tokenInt,tokenInt,tokenInt,... OriginalText
    job0b.setMapOutputValueClass(NullWritable.class);

    job0b.setNumReduceTasks(0);
    
    FileInputFormat.addInputPath(job0b, rawWithRidPath);
    FileOutputFormat.setOutputPath(job0b, newInputPath); // Format: RID tokenInt,tokenInt,tokenInt,... OriginalText

    job0b.waitForCompletion(true);
    
    // delete intermediate results:
    filesystem.delete(tokenStringPath, true);
    filesystem.delete(rawWithRidPath, true);
 
    return 0;
  }
  
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new Configuration(), new EndToEndTokenizeJob(), args);
  }
}
