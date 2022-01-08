/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package de.huberlin.massjoin;

import de.huberlin.vernicajoin.IntPairWritable;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 *
 * @author fabi
 */
public class MassJoinS3InputFormat extends FileInputFormat<IntPairWritable, Text> {

  @Override
  public RecordReader<IntPairWritable, Text> createRecordReader(InputSplit is, TaskAttemptContext tac) throws IOException, InterruptedException {
    return new S3RecordReader();
  }

}
