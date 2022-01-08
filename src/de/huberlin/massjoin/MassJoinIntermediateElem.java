package de.huberlin.massjoin;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import org.apache.hadoop.io.WritableComparable;

public final class MassJoinIntermediateElem implements WritableComparable<MassJoinIntermediateElem> {
    private int[] filterTokens;
    private int partition; // 0: R, 1: S
    private int segment;
    private int abs;
    private int id;

    MassJoinIntermediateElem() {}
    
    public MassJoinIntermediateElem(int partition, int segment, int abs, int id, int[] filterTokens) {
      set(partition, segment, abs, id, filterTokens);
    }
    
    public void set(int partition, int segment, int abs, int id, int[] filterTokens) {
      this.partition = partition;
      this.segment = segment;
      this.abs = abs;
      this.id = id;
      this.filterTokens = filterTokens;
    }

    public int getPartition() {
      return partition;
    }

    public int getSegment() {
      return segment;
    }

    public int getAbs() {
      return abs;
    }

    public int getId() {
      return id;
    }
    
    public int[] getFilterTokens() {
      return filterTokens;
    }

    // readFields() and write() are used within Hadoop to serialize and unserialize the objects
    @Override
    public void readFields(DataInput in) throws IOException {
      partition = in.readInt();
      segment = in.readInt();
      abs = in.readInt();
      id = in.readInt();
      int numberOfTokens = in.readInt();
      filterTokens = new int[numberOfTokens];
      for (int i = 0; i < numberOfTokens; i++) {
        filterTokens[i] = in.readInt();
      }
    }
    
    private String getTokensAsString() {
      String outString = "";
      for (int i = 0; i < filterTokens.length; i++) {
        if (!outString.equals("")) {
          outString += ",";
        }
        outString += filterTokens[i];
      }
      return outString;
    }

    @Override
    public void write(DataOutput out) throws IOException {
      out.writeInt(partition);
      out.writeInt(segment);
      out.writeInt(abs);
      out.writeInt(id);
      out.writeInt(filterTokens.length);
      for (int i = 0; i < filterTokens.length; i++) {
        out.writeInt(filterTokens[i]);
      }
    }

    @Override
    public int compareTo(MassJoinIntermediateElem o) {
      return (id < o.getId() ? -1 : (id == o.getId() ? 0 : 1));
    }

    @Override
    public String toString() {
      return "partition=" + partition + ", segment=" + segment + ", abs=" + abs + ", id=" + id + ", filterTokens=" + getTokensAsString();
    }
    
    @Override
    public MassJoinIntermediateElem clone() throws CloneNotSupportedException {
      return new MassJoinIntermediateElem(partition, segment, abs, id, Arrays.copyOf(filterTokens, filterTokens.length));
//      return (MassJoinIntermediateElem) super.clone();
    }

  }
