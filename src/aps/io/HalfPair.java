package aps.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class HalfPair implements Writable {
    private int id;
    private int vectorSize;
    private int overlap; // benötigt für combine

    public int getID() {
      return id;
    }
    
    public int getVectorSize() {
      return vectorSize;
    }
    
    public int getOverlap() {
      return overlap;
    }

    public void set(int id, int overlap, int vectorSize) {
      this.id = id;
      this.vectorSize = vectorSize;
      this.overlap = overlap;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        id = in.readInt();
        vectorSize = in.readInt();
        overlap = in.readInt();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(id);
        out.writeInt(vectorSize);
        out.writeInt(overlap);
    }

    @Override
    public boolean equals(Object o) {
        if (o == this)
            return true;
        if (o instanceof HalfPair) {
            HalfPair hp = (HalfPair) o;
            return id == hp.id;
        }
        return false;
    }

    @Override
    public String toString() {
        return id + "\t" + vectorSize;
    }
}
