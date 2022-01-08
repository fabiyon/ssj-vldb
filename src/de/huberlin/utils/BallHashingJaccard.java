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
public class BallHashingJaccard {
  double similarity;
  int upperBound;
  int lowerBound;
  TreeMap<IntegerComparable, Double> res;
  int[] universe;
  IntegerComparable record;
  
  public BallHashingJaccard(double similarity, int[] universe) {
    this.similarity = similarity;
    this.universe = universe;
  }
  
  public int[][] computeBall(int[] inputRecord) {
    record = new IntegerComparable(inputRecord);
    // compute static values:
    upperBound = (int)Math.floor((double)record.length / similarity);
    lowerBound = (int)Math.ceil((double)record.length * similarity);
    
    // Die Differenz (oder das Maximum?) aus upper und lowerBound ist gleich die Hamming-Distanz d. Das ist die
    // Anzahl der "Bits", die man verändern darf, damit ein Record noch ähnlich ist. 
    
    // compute unusedUniverse:
    ArrayList<Integer> unusedUniverseArrayList = new ArrayList();
    // füge alle aus dem Universe ein außer dem inputRecord:
    for (int i = 0; i < universe.length; i++) {
      int currentToken = universe[i];
      if (Arrays.binarySearch(inputRecord, currentToken) < 0) {
        unusedUniverseArrayList.add(currentToken);
      }
    }
    
    res = new TreeMap();
    res.put(record, 1.0);
    int oldSize = 0;
    do {
      oldSize = res.size();
      delete(record.tokens);
      insert(unusedUniverseArrayList, record.tokens);
    } while (res.size() != oldSize);
    
    int numberOfMatches = 0;
    for(Map.Entry<IntegerComparable,Double> entry : res.entrySet()) {
      if (entry.getValue() >= similarity) {
        numberOfMatches++;
      }
    }
    
    int[][] output = new int[numberOfMatches][];
    int count = 0;
    for(Map.Entry<IntegerComparable,Double> entry : res.entrySet()) {
      if (entry.getValue() >= similarity) {
        output[count++] = entry.getKey().tokens;
      }
    }
    return output;
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
    int[] universe = {1, 2, 3, 4, 5, 354480, 379303, 398506, 446888, 452459, 455016, 460937, 463529, 472004, 474879, 475164, 478591, 479577, 480219, 481308, 482282, 482562, 482585, 482623, 483574, 483845, 483967, 484826, 484915, 485031, 485056, 485186, 485263, 485332, 485362, 485446, 485458, 485465, 485491, 485659, 485672, 485749, 485766, 485911, 485918, 486005, 486019, 486022, 486053, 486059, 486113, 486125, 486196, 486206, 486213, 486246, 486283, 486309, 486310, 486315, 486328, 486343, 486396, 486399, 486411, 486414, 486438, 486444, 486447, 486448, 486450, 486453, 486459, 486462, 486464, 486465, 486466, 486467};
    int[] inputRecord = {1, 2, 3, 4, 5};
    BallHashingJaccard bhj = new BallHashingJaccard(.9, universe);
    int[][] result = bhj.computeBall(inputRecord);
    
    for (int[] currentRecord : result) {
      System.out.println(Arrays.toString(currentRecord));
    }
  }
  
  public double getJaccardSimilarityBetween(int[] a, int[] b) {
    double intersection = 0;
    for (int i = 0; i < a.length; i++) {
      for (int j = 0; j < b.length; j++) {
        if (b[j] == a[i]) {
          intersection++;
        }
      }
    }
    double result = (double) (intersection / (a.length + b.length - intersection));
    return result; 
  }
  
  public void delete(int[] record) {
    TreeMap<IntegerComparable, Double> newEntries = new TreeMap();
    for (Map.Entry<IntegerComparable, Double> entry : res.entrySet()) {
      int[] currentRecord = entry.getKey().tokens;
      Double currentSimilarity = entry.getValue();
      int potentialNewRecordLength = currentRecord.length - 1;
      if (potentialNewRecordLength < lowerBound) {
        return;
      }
      if (currentSimilarity < similarity) { // ich bin mir nicht ganz sicher bei diesem Abbruchkriterium: Bei Kindergartenbeispielen funktionierts. Wenn Ergebnisse fehlen, rausnehmen.
        return;
      }
      
      // lösche an jeder denkbaren Position:
      for (int deletePos = 0; deletePos < currentRecord.length; deletePos++) {
        int[] potentialNewRecord = new int[potentialNewRecordLength]; // scheiss Java
        int readPos = 0;
        for (int writePos = 0; writePos < potentialNewRecordLength; writePos++) {
          if (readPos == deletePos) {
            readPos++;
          }
          potentialNewRecord[writePos] = currentRecord[readPos++];
        }
        newEntries.put(new IntegerComparable(potentialNewRecord), getJaccardSimilarityBetween(potentialNewRecord, record));
      }      
    }
    res.putAll(newEntries);
  }
  
  public void insert(ArrayList<Integer> unusedUniverse, int[] record) {
    TreeMap<IntegerComparable, Double> newEntries = new TreeMap();
    for (Map.Entry<IntegerComparable, Double> entry : res.entrySet()) {
      int[] currentRecord = entry.getKey().tokens;
      Double currentSimilarity = entry.getValue();
      int potentialNewRecordLength = currentRecord.length + 1;
      if (potentialNewRecordLength > upperBound) {
        return;
      }
      if (currentSimilarity < similarity) { // ich bin mir nicht ganz sicher bei diesem Abbruchkriterium: Bei Kindergartenbeispielen funktionierts. Wenn Ergebnisse fehlen, rausnehmen.
        return;
      }
      
      // füge jedes mögliche Token ein aus dem unusedUniverse (genau eins):
      outer:
      for (int potentialNewToken : unusedUniverse) {
        int[] potentialNewRecord = new int[potentialNewRecordLength]; // scheiss Java
        
        // sortiertes Einfügen:
        int readPos = 0;
        boolean isAlreadyInserted = false;
        for (int writePos = 0; writePos < potentialNewRecordLength; writePos++) { // 1 2 3 4
          int readToken = Integer.MAX_VALUE;
          if (readPos < currentRecord.length) {
            readToken = currentRecord[readPos];
          }
          if (potentialNewToken == readToken) { // Token ist schon drin, mach mit dem nächsten potentiellen neuen Token weiter
            continue outer; 
          } else if (potentialNewToken < readToken && !isAlreadyInserted) {
            potentialNewRecord[writePos++] = potentialNewToken;
            isAlreadyInserted = true;
          }
          if (writePos < potentialNewRecordLength) {
            potentialNewRecord[writePos] = currentRecord[readPos];
          }
          readPos++;
        }
        newEntries.put(new IntegerComparable(potentialNewRecord), getJaccardSimilarityBetween(potentialNewRecord, record));
      }
    }
    res.putAll(newEntries);
  }
  
}
