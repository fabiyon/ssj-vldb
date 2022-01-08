package aps.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;

public class VectorComponentArrayWritable extends ArrayWritable {

  public VectorComponentArrayWritable() {
    super(VectorComponent.class);
  }

  public VectorComponentArrayWritable(VectorComponent[] values) {
    super(VectorComponent.class, values);
  }

  /**
   * Copy constructor. The values are deep copied.
   *
   * @param copy
   */
  public VectorComponentArrayWritable(VectorComponentArrayWritable copy) {
    super(VectorComponent.class, Arrays.copyOf(copy.get(), copy.get().length, VectorComponent[].class));
  }

  @Override
  public void set(Writable[] values) {
    assert (values instanceof VectorComponent[]);
    super.set(values);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
  }

  public VectorComponent[] toVectorComponentArray() {
    return (VectorComponent[]) toArray();
  }

  /**
   * returns the length of the vector
   *
   * @return the length
   */
  public int length() {
    return this.get().length;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    for (String s : this.toStrings()) {
      sb.append(s);
      sb.append('\n');
    }
    return sb.toString();
  }
}
