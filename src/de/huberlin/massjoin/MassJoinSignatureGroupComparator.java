/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package de.huberlin.massjoin;

import org.apache.hadoop.io.WritableComparator;

/**
 *
 * @author fabi
 */
public class MassJoinSignatureGroupComparator extends WritableComparator {

  public MassJoinSignatureGroupComparator() {
    super(MassJoinSignatureKey.class);
  }

  @Override
  public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
    int firstValue = readInt(b1, s1);
    int secondValue = readInt(b2, s2);
    // check length:
    if (firstValue < secondValue) {
      return -1;
    } else if (firstValue > secondValue) {
      return 1;
    }
    for (int offset = 4; offset < l1 - 4 && offset < l2 - 4; offset += 4) {
      firstValue = readInt(b1, s1 + offset);
      secondValue = readInt(b2, s2 + offset);
      
      if (firstValue < secondValue) {
        return -1;
      } else if (firstValue > secondValue) {
        return 1;
      }
    }
    return 0;
    
////    if (s1 < b1.length - 4 && s2 < b2.length - 4) {
//      int firstValue = readInt(b1, s1);
//      int secondValue = readInt(b2, s2);
//      if (firstValue < secondValue) { // der allererste Wert ist die Länge, d. h. wenn zwei Records die gleiche Länge haben wird erstmal iteriert
//        return -1;
//      } else if (firstValue > secondValue) {
//        return 1;
//      } else {
//        s1 = s1 + 4;
//        s2 = s2 + 4;
//        if (s1 < l1 - 4 && s2 < l2 - 4) { // we gotta ignore the last integer, because it contains the partition information (0: R, 1: S)
//          return compare(b1, s1, l1, b2, s2, l2);
//        }
//      }
////    }
//    return 0;
  }

}
