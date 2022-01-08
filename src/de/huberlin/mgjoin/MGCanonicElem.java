package de.huberlin.mgjoin;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.TreeMap;
import org.apache.hadoop.io.WritableComparable;

public class MGCanonicElem implements WritableComparable<MGCanonicElem> {
    public int[] tokens; // original token set. we take this as "random" order and use it as prefix for replication. we don't transmit this token set!
    private int key;
    public int prefixLength;
    public int[] tokensOrdered; // token set sorted by global token order (ascending frequency)

    MGCanonicElem() {}
    
    MGCanonicElem(MGCanonicElem e) {
      key = e.getKey();
      tokensOrdered = Arrays.copyOf(e.tokensOrdered, e.tokensOrdered.length);
      prefixLength = e.prefixLength;
    }
    
    MGCanonicElem(String text) {
      String[] textArr = text.split("\\s+");
      if (textArr.length > 0) {
        key = Integer.parseInt(textArr[0]);
      }
      if (textArr.length > 1) {
        tokens = this.toTokenArr(textArr[1]);
      } else {
        tokens = new int[0];
      }
    }
    
    public void setKey(int key) {
      this.key = key;
    }
    
    public void setPrefixLength(int prefixLength) {
      this.prefixLength = prefixLength;
    }
    
    // generate copy of token set and order it by the global token ordering:
    public void sortByGlobalTokenOrder(TreeMap<Integer, Integer> tokenFreq) {
      tokensOrdered = new int[tokens.length];
      int cnt = 0;

      for (int t : tokens) {
        tokensOrdered[cnt++] = tokenFreq.get(t); // this does not change the order yet, but assigns new token IDs with integers of ascending order
      }

      Arrays.sort(tokensOrdered); // this actually orders them according to the global token order
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

    public int getKey() {
      return key;
    }

    // readFields() and write() are used within Hadoop to serialize and unserialize the objects
    @Override
    public void readFields(DataInput in) throws IOException {
      key = in.readInt();
      prefixLength = in.readInt();
      tokensOrdered = new int[in.readInt()];
      for (int i = 0; i < tokensOrdered.length; i++) {
        tokensOrdered[i] = in.readInt();
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
      out.writeInt(key);
      out.writeInt(prefixLength);
      out.writeInt(tokensOrdered.length);
      for (int i = 0; i < tokensOrdered.length; i++) {
        out.writeInt(tokensOrdered[i]);
      }
    }

    @Override
    public int compareTo(MGCanonicElem o) {
      long thisValue = this.key;
      long thatValue = o.key;
      return (thisValue < thatValue ? -1 : (thisValue==thatValue ? 0 : 1));
    }

    @Override
    public String toString() {
      return key + " " + getTokensAsString();
    }
    
    public boolean hasPrefixIntersectWith(MGCanonicElem e) { // beachte nur die Präfixe!
      int i = 0;
      int j = 0;
      
      // forward (filter by least frequent tokens):
      for (; i < this.prefixLength && j < e.prefixLength; ) {
        if (e.tokensOrdered[i] == this.tokensOrdered[j]) {
          return true;
        } else if (e.tokensOrdered[i] < this.tokensOrdered[j]) {
          i++;
        } else {
          j++;
        }
      }
      
      // backward (filter by most frequent tokens):
      j = this.tokensOrdered.length - this.prefixLength;
      i = e.tokensOrdered.length - e.prefixLength;
      
      for (; i < e.tokensOrdered.length && j < this.tokensOrdered.length; ) {
        if (e.tokensOrdered[i] == this.tokensOrdered[j]) {
          return true;
        } else if (e.tokensOrdered[i] < this.tokensOrdered[j]) {
          i++;
        } else {
          j++;
        }
      }

      return false;
    }
    
    public double getJaccardSimilarityBetween(MGCanonicElem o) {
      double intersection = 0;
      if (this.tokensOrdered != null && o.tokensOrdered != null) {
        int i = 0; // für this
        int j = 0; // für o
        for (; i < this.tokensOrdered.length && j < o.tokensOrdered.length; ) {
          if (this.tokensOrdered[i] == o.tokensOrdered[j]) {
            intersection++;
            i++;
            j++;
          } else if (this.tokensOrdered[i] < o.tokensOrdered[j]) {
            i++;
          } else {
            j++;
          }
        }
        double outVal = (double) (intersection / (this.tokensOrdered.length + o.tokensOrdered.length - intersection));
        return outVal; 
      }
      return 0;
    }
  }