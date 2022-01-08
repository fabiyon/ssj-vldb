/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package edu.uci.ics.fuzzyjoin.similarity;

/**
 *
 * @author fabi
 */
public class SimilarityFiltersOverlap implements SimilarityFilters {
  int overlap;

  SimilarityFiltersOverlap(int i) {
    overlap = i;
  }

  @Override
  public int getLengthLowerBound(int length) {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public int getLengthUpperBound(int length) {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public int getPrefixLength(int length) {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public boolean passLengthFilter(int lengthX, int lengthY) {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public boolean passPositionFilter(int noGramsCommon, int positionX, int lengthX, int positionY, int lengthY) {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public float passSimilarityFilter(int[] tokensX, int startX, int lengthX, int prefixLengthX, int[] tokensY, int startY, int lengthY, int prefixLengthY, int intersectionSizePrefix) {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public float passSimilarityFilter(int[] tokensX, int prefixLengthX, int[] tokensY, int prefixLengthY, int intersectionSizePrefix) {
    float similarity = intersectionSizePrefix;
    int x = prefixLengthX;
    int y = prefixLengthY;
    
    // we need to move back in order to not miss some overlap, i. e.
    // r0: 1 5 6 | 8 9
    // r3: 1 2 4 | 6 8
    // Prefix length = 3 for l = 1. Prefix overlap would be 1, which is correct.
    // We would calculate an overlap of 2 here, although 3 would be correct.
    
    if (tokensX[x] > tokensY[y]) { // there might be elements in the prefix of x we missed:
      while (tokensX[x] > tokensY[y]) {
        x--;
      }
    } else if (tokensX[x] < tokensY[y]) { // there might be elements in the prefix of y we missed:
      while (tokensX[x] < tokensY[y]) {
        y--;
      }
    }

    
    xloop:
    for (; x < tokensX.length; x++) {
      for (; y < tokensY.length; y++) {
        if (tokensX[x] < tokensY[y]) {
          // move forward in the xloop
          continue xloop;
        } else if (tokensX[x] == tokensY[y]) {
          similarity++;
        } 
        // otherwise move forward in the yloop
      }
      
    }
    
    if (similarity < overlap) {
      return 0;
    }
    
    return similarity;
  }

  @Override
  public boolean passSuffixFilter(int[] tokensX, int startX, int lengthX, int positionX, int[] tokensY, int startY, int lengthY, int positionY) {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public boolean passSuffixFilter(int[] tokensX, int positionX, int[] tokensY, int positionY) {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }
  
}
