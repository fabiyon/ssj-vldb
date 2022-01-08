package aps.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class VectorComponent implements WritableComparable<VectorComponent> {
  private int id;

  public VectorComponent() {
  }

  public VectorComponent(int id) {
    this.id = id;
  }

  public int getID() {
    return id;
  }

  /**
   * Defines the natural ordering for VectorComponents. VectorComponents sort
   * descending, from most frequent to least frequent. This method is
   * inconsistent with equals because it does not take the weight into
   * consideration.
   */
  @Override
  public int compareTo(VectorComponent other) {
    // sorts descending, inconsistent with equals
    return this.id == other.id ? 0 : this.id < other.id ? 1 : -1; // <<<<<<<<<<<<<<< eigenwillige Interpretation von größer und kleiner
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    id = in.readInt();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(id);
  }

  @Override
  public String toString() {
    return id + "";
  }

  @Override
  public int hashCode() {
    int result = 17;
    result = 31 * result + (int) (id ^ (id >>> 32));
    return result;
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof VectorComponent) {
      VectorComponent vc = (VectorComponent) o;
      return (this.id == vc.id);
    }
    return false;
  }
}
