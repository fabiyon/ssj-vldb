/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package de.huberlin.vsmart;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 *
 * @author fabi
 */
public class VsmartJoinS2InputFormat extends FileInputFormat<IntWritable, Text> {

  @Override
  public RecordReader<IntWritable, Text> createRecordReader(InputSplit is, TaskAttemptContext tac) throws IOException, InterruptedException {
    return new S2RecordReader();
  }

}
