package de.huberlin.massjoin;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import java.util.Arrays;

public class MassJoinSignatureKey implements WritableComparable<MassJoinSignatureKey> {
  private int[] tokens;
  private int partition; // 0: R, 1: S

  public MassJoinSignatureKey() {
  }

  public MassJoinSignatureKey(int[] tokens, int partition) {
    set(tokens, partition);
  }

  public int[] getTokens() {
    return tokens;
  }
  
  public int getLength() {
    return tokens.length;
  }

  public void set(int[] tokens, int partition) {
    this.tokens = tokens;
    this.partition = partition;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    int numberOfTokens = in.readInt();
    tokens = new int[numberOfTokens];
    for (int i = 0; i < numberOfTokens; i++) {
      tokens[i] = in.readInt();
    }
    partition = in.readInt();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(tokens.length);
    for (int i = 0; i < tokens.length; i++) {
      out.writeInt(tokens[i]);
    }
    out.writeInt(partition);
  }

  @Override
  public int compareTo(MassJoinSignatureKey other) { // irgendwas sorgt hier dafür, dass die gleichen Keys nicht matchen...
//    if (Arrays.equals(tokens, other.getTokens())) {
//      return 0;
//    }
    // check length:
    if (tokens.length < other.getLength()) {
      return -1;
    } else if (tokens.length > other.getLength()) {
      return 1;
    }
    for (int i = 0; i < tokens.length; i++) {
      if (tokens[i] < other.getTokens()[i]) {
        return -1;
      } else if (tokens[i] > other.getTokens()[i]) {
        return 1;
      }
    }
    return 0;
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof MassJoinSignatureKey) {
      MassJoinSignatureKey k = (MassJoinSignatureKey) other;
      return (this.compareTo(k) == 0);
    }
    return false;
  }

  @Override
  public int hashCode() {
    int hash = 371 + Arrays.hashCode(this.tokens);
    return hash;
  }

  @Override
  public String toString() {
    String buffer = "";
    for (int i = 0; i < tokens.length; i++) {
      buffer += " " + tokens[i];
    }
    return buffer;
  }
}
