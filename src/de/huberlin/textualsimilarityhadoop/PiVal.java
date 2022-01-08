package de.huberlin.textualsimilarityhadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import org.apache.hadoop.io.WritableComparable;

public class PiVal implements WritableComparable<PiVal> {
    public int rid;
    public double[] pivotDistance;
    public int[] tokens;

    PiVal() {}
    
    public PiVal(String text, int size) {
      String[] textArr = text.split("\\s+");
      if (textArr.length > 0) {
        rid = Integer.parseInt(textArr[0]);
      }
      if (textArr.length > 1) {
        tokens = this.toTokenArr(textArr[1]);
      } else {
        tokens = new int[0];
      }
      pivotDistance = new double[size];
    }
    
//    public PiVal(int key, int size) {
//      this.rid = key;
//      pivotDistance = new double[size];
//    }
    
    public PiVal(PiVal p) {
      this.rid = p.rid;
      this.pivotDistance = Arrays.copyOf(p.pivotDistance, p.pivotDistance.length);
      this.tokens = Arrays.copyOf(p.tokens, p.tokens.length);
    }
    
    private int[] toTokenArr(String input) {
      String[] tokenStringArr = input.split(",");
      int[] tokenArr = new int[tokenStringArr.length];
      int count = 0;
      for (String token : tokenStringArr) {
        if (!token.equals("")) {
          tokenArr[count++] = Integer.parseInt(token);
        }
      }
      return tokenArr;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      rid = in.readInt();
      int numberOfPivots = in.readInt();
      pivotDistance = new double[numberOfPivots];
      for (int i = 0; i < numberOfPivots; i++) {
        pivotDistance[i] = in.readDouble();
      }
      tokens = new int[in.readInt()];
      for (int i = 0; i < tokens.length; i++) {
        tokens[i] = in.readInt();
      }
    }

    @Override
    public void write(DataOutput out) throws IOException {
      out.writeInt(rid);
      out.writeInt(pivotDistance.length);
      for (int i = 0; i < pivotDistance.length; i++) {
        out.writeDouble(pivotDistance[i]);
      }
      out.writeInt(tokens.length);
      for (int i = 0; i < tokens.length; i++) {
        out.writeInt(tokens[i]);
      }
    }

    // Brauchen wir eigentlich für nichts...
    @Override
    public int compareTo(PiVal o) {
      if (this.rid < o.rid) {
        return -1;
      } else if (this.rid > o.rid) {
        return 1;
      } else {
        if (this.pivotDistance.length < o.pivotDistance.length) {
          return -1;
        } else if (this.pivotDistance.length > o.pivotDistance.length) {
          return 1;
        } else {
          // müsste man implementieren, wenn mans denn bräuchte
        }
      }
      
      return 0;
    }

    @Override
    public String toString() {
      return rid + Arrays.toString(pivotDistance);
    }
    
    public double getJaccardDistanceBetween(PiVal o) {
      double intersection = 0;
      if (this.tokens != null && o.tokens != null) {
        for (int i = 0; i < this.tokens.length; i++) {
          for (int j = 0; j < o.tokens.length; j++) {
            if (o.tokens[j] == this.tokens[i]) {
              intersection++;
            }
          }
        }
        return 1 - (double) (intersection / (this.tokens.length + o.tokens.length - intersection)); 
      }
      return 1;
    }

  }
