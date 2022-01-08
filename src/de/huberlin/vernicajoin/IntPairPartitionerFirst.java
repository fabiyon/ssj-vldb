package de.huberlin.vernicajoin;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Partitioner;

public class IntPairPartitionerFirst extends Partitioner<IntPairWritable, Object> implements Configurable {

  private Configuration configuration;

  @Override
  public int getPartition(IntPairWritable key, Object value, int numPartitions) {
    return key.getFirst() % numPartitions;
  }

  @Override
  public void setConf(Configuration conf) {
    this.configuration = conf;
  }

  @Override
  public Configuration getConf() {
    return configuration;
  }

}

