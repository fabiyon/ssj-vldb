/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package de.huberlin.textualsimilarityhadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;

/**
 *
 * @author fabi
 */
public class StringElem implements WritableComparable<StringElem> {
    private int[] tokens;
    public long partitionID = -1;
    private long windowID = -1;
    private long key;
    private long prevPartition = -1;

    StringElem() {}
    
    public StringElem(String text) {
      String[] textArr = text.split("\\s+");
      if (textArr.length > 0) {
        key = Long.parseLong(textArr[0]);
      }
      if (textArr.length > 1) {
        tokens = this.toTokenArr(textArr[1]);
      } else {
        tokens = new int[0];
      }
      if (textArr.length > 2) { // the last element resembles the prevPartition:
        prevPartition = Long.parseLong(textArr[2]);
      }
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

    public int getSizeInBytes() { 
      return 32 + // 4 long variables à 64 bits
              tokens.length * 4;// + // tokens.length 32 bits = 4 bytes
    }

    public long getKey() {
      return key;
    }

    public void setPartitionID(long partitionID) {
      this.partitionID = partitionID;
    }

    public long getPartitionID() {
      return partitionID;
    }

    public long getWindowID() {
      return windowID;
    }

    public void setWindowID(long windowID) {
      this.windowID = windowID;
    }

    public long getPrevPartition() {
      return prevPartition;
    }

    public void setPrevPartition(long prevPartition) {
      this.prevPartition = prevPartition;
    }

    public double getJaccardDistanceBetween(StringElem o) {
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
    
    public double getCosineDistanceBetween(StringElem o) {
      // Summe des Punktprodukts:
      double intersection = 0;
      if (this.tokens != null && o.tokens != null) {
        for (int i = 0; i < this.tokens.length; i++) {
          for (int j = 0; j < o.tokens.length; j++) {
            if (o.tokens[j] == this.tokens[i]) {
              intersection++;
            }
          }
        }
        double singleRes = (1.0 / this.tokens.length) * (1.0 / o.tokens.length);
        return Math.pow(singleRes, intersection);
      }
      return 1;
    }
    
    public double getEuclidianDistanceBetween(StringElem o) {
      // Summe des Punktprodukts:
      double intersection = 0;
      if (this.tokens != null && o.tokens != null) {
        for (int i = 0; i < this.tokens.length; i++) {
          for (int j = 0; j < o.tokens.length; j++) {
            if (o.tokens[j] == this.tokens[i]) {
              intersection++;
            }
          }
        }
        double totalLength = this.tokens.length + o.tokens.length - intersection;
        double nonOverlapping = totalLength - intersection;
        double dist = Math.sqrt(nonOverlapping) / totalLength;
        System.out.println("dist=" + dist);
        return dist;
      }
      return 1;
    }

    // readFields() and write() are used within Hadoop to serialize and unserialize the objects
    @Override
    public void readFields(DataInput in) throws IOException {
      partitionID = in.readLong();
      windowID = in.readLong();
      key = in.readLong();
      tokens = new int[in.readInt()];
      for (int i = 0; i < tokens.length; i++) {
        tokens[i] = in.readInt();
      }
      prevPartition = in.readLong();
    }
    
    private String getTokensAsString() {
      String outString = "";
      for (int i = 0; i < tokens.length; i++) {
        if (!outString.equals("")) {
          outString += ",";
        }
        outString += tokens[i];
      }
      return outString;
    }

    @Override
    public void write(DataOutput out) throws IOException {
      out.writeLong(partitionID);
      out.writeLong(windowID);
      out.writeLong(key);
      out.writeInt(tokens.length);
      for (int i = 0; i < tokens.length; i++) {
        out.writeInt(tokens[i]);
      }
      out.writeLong(prevPartition);
    }

    @Override
    public int compareTo(StringElem o) {
      long thisValue = this.key;
      long thatValue = o.key;
      return (thisValue < thatValue ? -1 : (thisValue==thatValue ? 0 : 1));
    }

    @Override
    public String toString() {
      return key + " " + getTokensAsString();
    }

    public String toStringPart() {
      return this.toString() + " " + partitionID;
    }

    public String toStringPrev() {
      return this.toString() + " " + prevPartition;
    }
  }// end StringElem
