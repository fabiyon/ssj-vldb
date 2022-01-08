package aps.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Partitioner;

public class GenericKey implements WritableComparable<GenericKey> {
  private int primary;
  private int secondary;
  
  public static class StripePartitioner extends Partitioner<GenericKey, GenericValue> implements Configurable {
    private Configuration configuration;

    @Override
    public int getPartition(GenericKey key, GenericValue value, int numPartitions) {
      return (key.primary % numPartitions); // we only consider the primary key: this assures that the needed records are in the same partition
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
  
  
  /**
   * Used for grouping
   */
  public static class PrimaryComparator extends WritableComparator {

    public PrimaryComparator() {
      super(GenericKey.class);
    }

    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
      int firstValue = readInt(b1, s1);
      int secondValue = readInt(b2, s2);
      int result = (firstValue < secondValue ? -1 : (firstValue == secondValue ? 0 : 1));
      return result;
    }
  }

  public static class Comparator extends WritableComparator {

    public Comparator() {
      super(GenericKey.class);
    }

    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
      int firstValue = readInt(b1, s1);
      int secondValue = readInt(b2, s2);
      int result = (firstValue < secondValue ? -1 : (firstValue == secondValue ? 0 : 1));
      if (result == 0) { // this is important: we sort the records by the secondary key. -1 has to be first
        firstValue = readInt(b1, s1 + 4); // die SIZE ist in Bits gespeichert, während der Offset in Bytes angegeben werden muss, also durch 8
        secondValue = readInt(b2, s2 + 4);
        result = (firstValue < secondValue ? -1 : (firstValue == secondValue ? 0 : 1));
      }
      return result; // ist 1 wenn firstValue > secondValue, sonst -1
    }
  }

  public int getPrimary() {
    return primary;
  }
  
  public int getSecondary() {
    return secondary;
  }

  public void set(int primary, int secondary) {
    this.primary = primary;
    this.secondary = secondary;
  }

  
  public void set(int primary) { // used for LPD
    this.primary = primary;
    this.secondary = -1; // represents the star
  }
  
  @Override
  public void readFields(DataInput in) throws IOException {
    primary = in.readInt();
    secondary = in.readInt();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(primary);
    out.writeInt(secondary);
  }

  @Override
  public int compareTo(GenericKey other) {
    int result = (this.primary < other.primary ? -1 : (this.primary == other.primary ? 0 : 1));
    if (result == 0) {
      result = (this.secondary < other.secondary ? -1 : (this.secondary == other.secondary ? 0 : 1));
    }
    return result;
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof GenericKey) {
      GenericKey k = (GenericKey) other;
      return (this.compareTo(k) == 0);
    }
    return false;
  }

  @Override
  public String toString() {
    return primary + "\t" + secondary;
  }
}
