/**
 * Copyright 2010-2011 The Regents of the University of California
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS"; BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 * Borrowed from and modified
 * Author: Rares Vernica <rares (at) ics.uci.edu>
 */
package de.huberlin.vernicajoin;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class IntPairWritable implements WritableComparable {

  public static int[] PRIME = new int[]{17, 31}; // used for hashCode
  protected int first;
  protected int second;

  public IntPairWritable() {
  }

  public IntPairWritable(int first, int second) {
    this.first = first;
    this.second = second;
  }

  public int getFirst() {
    return first;
  }

  public int getSecond() {
    return second;
  }

  public void set(int first, int second) {
    this.first = first;
    this.second = second;
  }

  public void setFirst(int first) {
    this.first = first;
  }

  public void setSecond(int second) {
    this.second = second;
  }

  /**
   * A Comparator optimized for IntPairWritable. <<<< this is a bit too optimized compared to the other implementations, so we disable it
   */
//  public static class Comparator extends WritableComparator {
//
//    public Comparator() {
//      super(IntPairWritable.class);
//    }
//
//    @Override
//    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
//      /*
//       * TODO one compareBytes call with all the bytes is enough
//       */
//      int c = WritableComparator.compareBytes(b1, s1, 4, b2, s2, 4);
//      if (c != 0) {
//        return c;
//      }
//      return WritableComparator
//              .compareBytes(b1, s1 + 4, 4, b2, s2 + 4, 4);
//    }
//  }
//
//  static { // register this comparator
//    WritableComparator.define(IntPairWritable.class, new Comparator());
//  }

  @Override
  public int compareTo(Object o) {
    if (this == o) {
      return 0;
    }
    IntPairWritable p = (IntPairWritable) o;
    if (first != p.first) {
      return first < p.first ? -1 : 1;
    }
    return second < p.second ? -1 : second > p.second ? 1 : 0;
  }
  
  @Override
  public boolean equals(Object o) {
    if (o == null) {
      return false;
    }
    if (this == o) {
      return true;
    }
    if (!(o instanceof IntPairWritable)) {
      return false;
    }
    IntPairWritable p = (IntPairWritable) o;
    return first == p.first && second == p.second;
  }
  
  @Override
  public int hashCode() {
    return first * PRIME[0] + second;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    first = in.readInt();
    second = in.readInt();
  }
  
  @Override
  public String toString() {
    return "(" + first + "," + second + ")";
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(first);
    out.writeInt(second);
  }
}
