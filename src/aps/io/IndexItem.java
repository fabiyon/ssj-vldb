package aps.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class IndexItem implements WritableComparable<IndexItem> {

  private int vectorID;
  private int maxIndexed;
  private int vectorLength;

  public IndexItem() {
  }

  public IndexItem(IndexItem other) {
    set(other);
  }

  public int getID() {
    return vectorID;
  }

  public void setMaxIndexed(int maxIndexed) {
    this.maxIndexed = maxIndexed;
  }

  public int getMaxIndexed() {
    return maxIndexed;
  }

  public void setVectorLength(int vectorLength) {
    this.vectorLength = vectorLength;
  }

  public int vectorLength() {
    return vectorLength;
  }

  public void set(int vectorID) {
    this.vectorID = vectorID;
  }

  public void set(IndexItem item) {
    set(item.vectorID, item.maxIndexed, item.vectorLength);
  }

  private void set(int vectorID, int maxIndexed, int length) {
    this.vectorID = vectorID;
    this.maxIndexed = maxIndexed;
    this.vectorLength = length;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    vectorID = in.readInt();
    maxIndexed = in.readInt();
    vectorLength = in.readInt();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(vectorID);
    out.writeInt(maxIndexed);
    out.writeInt(vectorLength);
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof IndexItem) {
      IndexItem ii = (IndexItem) o;
      return this.vectorID == ii.vectorID;
    }
    return false;
  }

  @Override
  public int hashCode() {
    int result = 17;
    result = 31 * result + (int) vectorID;
    return result;
  }

  @Override
  public String toString() {
    return vectorID + "\t " + ((maxIndexed == 0) ? "" : "[" + maxIndexed + "]");
  }

  @Override
  public int compareTo(IndexItem other) {
    int result = this.vectorID < other.vectorID ? -1 : this.vectorID > other.vectorID ? 1 : 0;
    return result; // 1 und -1 vertauscht, damit Arrays.sort die Liste aufsteigend sortiert
  }

  /* Static Methods */
  public static int getLeastPrunedVectorID(IndexItem firstItem, IndexItem secondItem) {
    int result = firstItem.getMaxIndexed() > secondItem.getMaxIndexed() ? firstItem.getID() : secondItem.getID();
    return result;
  }

  public static int getMostPrunedVectorID(IndexItem firstItem, IndexItem secondItem) {
    int result = firstItem.getMaxIndexed() > secondItem.getMaxIndexed() ? secondItem.getID() : firstItem.getID();
    return result;
  }
}
