package de.huberlin.vernicajoin;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
import org.apache.hadoop.io.WritableComparable;

public class VernicaElem implements WritableComparable<VernicaElem> {
    private int[] tokens;
    private long key;

    VernicaElem() {}
    
    VernicaElem(String text) {
      String[] textArr = text.split("\\s+");
      if (textArr.length > 0) {
        key = Long.parseLong(textArr[0]);
      }
      if (textArr.length > 1) {
        tokens = this.toTokenArr(textArr[1]);
      } else {
        tokens = new int[0];
      }
    }
    
    public void setTokens(TreeMap<Integer, Integer> sortedTokens) { // ist das Ergebnis immer korrekt sortiert? <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
      tokens = new int[sortedTokens.size()];
      int pos = 0;
      for(Map.Entry<Integer,Integer> entry : sortedTokens.entrySet()) {
        tokens[pos++] = entry.getKey(); // we simply replace the tokens with the position in the frequencies to make the PPJoin work
      }
    }
    
    public void setKey(long key) {
      this.key = key;
    }
    
    public int[] getTokens() {
      return tokens;
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
      return 8 + // 1 long variable à 64 bits
              tokens.length * 4;// + // tokens.length 32 bits = 4 bytes
    }

    public long getKey() {
      return key;
    }

    // readFields() and write() are used within Hadoop to serialize and unserialize the objects
    @Override
    public void readFields(DataInput in) throws IOException {
      key = in.readLong();
      tokens = new int[in.readInt()];
      for (int i = 0; i < tokens.length; i++) {
        tokens[i] = in.readInt();
      }
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
      out.writeLong(key);
      out.writeInt(tokens.length);
      for (int i = 0; i < tokens.length; i++) {
        out.writeInt(tokens[i]);
      }
    }

    @Override
    public int compareTo(VernicaElem o) {
      long thisValue = this.key;
      long thatValue = o.key;
      return (thisValue < thatValue ? -1 : (thisValue==thatValue ? 0 : 1));
    }

    @Override
    public String toString() {
      return key + " " + getTokensAsString();
    }
  }