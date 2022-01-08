/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package de.huberlin.utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;

/**
 *
 * @author fabi
 */
public class BallHashingHamming {
  double jaccardSimilarity;
  TreeMap<IntegerComparable, Integer> res;
  int[] universe;
  IntegerComparable record;
  
  public BallHashingHamming(double similarity, int[] universe) {
    this.jaccardSimilarity = similarity;
    this.universe = universe;
  }
  
  public int[][] computeBall(int[] inputRecord) {
    record = new IntegerComparable(inputRecord);

    res = new TreeMap();
    res.put(record, 0);
    int distance = (int)Math.ceil((1 - jaccardSimilarity) * record.length / 2);
    for (int currentDistance = 1; currentDistance <= distance; currentDistance++) {
      generateBallForDistance(currentDistance);
    }
    
    int[][] output = new int[res.size()][];
    int count = 0;
    for(Map.Entry<IntegerComparable,Integer> entry : res.entrySet()) {
      output[count++] = entry.getKey().tokens;
    }
    return output;
  }
  
  public int[] getLexicographicallyFirstSignature(int[] inputRecord) {
    record = new IntegerComparable(inputRecord);
    res = new TreeMap();
    res.put(record, 0);
    int distance = (int)Math.ceil((1 - jaccardSimilarity) * record.length / 2);
    generateBallForDistance(distance);
    for(Map.Entry<IntegerComparable,Integer> entry : res.entrySet()) { // <<<< VORSICHT: Das erste Element könnte auch der Record selbst sein!!!
      return entry.getKey().tokens; // we output the first entry. We already defined an order in the IntegerComparable class (lexicographically or not does not matter)
    }
    return new int[0];
  }
  
  private static class IntegerComparable implements Comparable {
    
    public int[] tokens;
    public int length;
    
    public IntegerComparable(int[] tokens) {
      this.tokens = tokens;
      this.length = tokens.length;
    }

    @Override
    public int compareTo(Object o) {
      IntegerComparable leftObject = this;
      IntegerComparable rightObject = (IntegerComparable)o;
      int leftLength = this.length;
      int rightLength = rightObject.length;
      for (int leftCounter = 0, rightCounter = 0; leftCounter < leftLength && rightCounter < rightLength;) {
        if (leftObject.tokens[leftCounter] < rightObject.tokens[rightCounter]) {
          leftCounter++;
          return -1;
        } else if (leftObject.tokens[leftCounter] == rightObject.tokens[rightCounter]) {
          leftCounter++;
          rightCounter++;
        } else {
          rightCounter++;
          // if right is over:
          return 1;
        }
      }
      if (leftLength < rightLength) {
        return -1; 
      } else if (rightLength < leftLength) {
        return 1;
      }
      return 0;
    }
}
  
  public static void main(String[] args) {
    int[] universe = {1, 2, 3, 4, 5};//, 398506, 446888, 452459, 455016, 460937, 463529, 472004, 474879, 475164, 478591, 479577, 480219, 481308, 482282, 482562, 482585, 482623, 483574, 483845, 483967, 484826, 484915, 485031, 485056, 485186, 485263, 485332, 485362, 485446, 485458, 485465, 485491, 485659, 485672, 485749, 485766, 485911, 485918, 486005, 486019, 486022, 486053, 486059, 486113, 486125, 486196, 486206, 486213, 486246, 486283, 486309, 486310, 486315, 486328, 486343, 486396, 486399, 486411, 486414, 486438, 486444, 486447, 486448, 486450, 486453, 486459, 486462, 486464, 486465, 486466, 486467};
    int[] inputRecord = {1, 2, 3, 4, 5};
    BallHashingHamming bhj = new BallHashingHamming(.8, universe);
    int[][] result = bhj.computeBall(inputRecord);
    
    for (int[] currentRecord : result) {
      System.out.println(Arrays.toString(currentRecord));
    }
  }
  
  public void generateBallForDistance(int currentDistance) {
    TreeMap<IntegerComparable, Integer> newEntries = new TreeMap();
    for (Map.Entry<IntegerComparable, Integer> entry : res.entrySet()) {
      int[] compareRecord = entry.getKey().tokens;
      Integer compareDistance = entry.getValue();
      if (compareDistance == currentDistance - 1) { // only use records from last iteration
        int newRecordLength = compareRecord.length - 1;
        // DELETE:
        for (int deletePos = 0; deletePos < compareRecord.length; deletePos++) {
          int[] potentialNewRecord = new int[newRecordLength];
          int readPos = 0;
          for (int writePos = 0; writePos < newRecordLength; writePos++) {
            if (readPos == deletePos) {
              readPos++;
            }
            potentialNewRecord[writePos] = compareRecord[readPos++];
          }
          newEntries.put(new IntegerComparable(potentialNewRecord), currentDistance);
        }
        
        // INSERT:
        newRecordLength = compareRecord.length + 1;
        outer:
        for (int insertToken : universe) {
          int[] potentialNewRecord = new int[newRecordLength];
          int readPos = 0;
          boolean alreadyInserted = false;
          for (int writePos = 0; writePos < newRecordLength; writePos++) {
            if (readPos >= compareRecord.length) { // we are at the end and the new token needs to be copied:
              potentialNewRecord[writePos] = insertToken;
              break;
            }
            int readToken = compareRecord[readPos];
            if (insertToken == readToken) {
              continue outer; // skip this token, because it is already contained
            } else if (insertToken < readToken && !alreadyInserted) {
              potentialNewRecord[writePos++] = insertToken;
              alreadyInserted = true;
            }
            if (writePos < newRecordLength) {
              potentialNewRecord[writePos] = readToken;
              readPos++;
            }
          }
          newEntries.put(new IntegerComparable(potentialNewRecord), currentDistance);
        }
        
      }
    }
    res.putAll(newEntries);
  }
  
}
