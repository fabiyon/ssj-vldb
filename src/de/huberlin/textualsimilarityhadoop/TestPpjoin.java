/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package de.huberlin.textualsimilarityhadoop;

import java.util.ArrayList;
import java.util.Arrays;

/**
 *
 * @author fabi
 */
public class TestPpjoin {
  public static void main(String[] args) {
    PPJoinPlus ppj = new PPJoinPlus((float) .95);
    ArrayList<Long> results = new ArrayList();
    
    int[] tokens1 = {0, 2, 3, 5, 7, 8, 12, 16, 18, 19, 23, 24, 25, 26, 28, 31, 33, 34, 36, 37}; // 1107254 <<< das ist völlig richtig: das seltenste Token steht vorne!
//    int[] tokens1 = {370904,444946,461939,469715,477400,481820,483870,485255,485448,485459,485937,485950,486132,486405,486444,486452,486458,486462,486466,486467}; // 1107254
    ppj.selfJoinAndAddRecord(tokens1, 1107254, results);
    
    int[] tokens = {2, 3, 5, 7, 8, 12, 16, 18, 19, 23, 24, 25, 26, 28, 31, 33, 34, 36, 37};
//    int[] tokens = {370904,444946,461939,469715,481820,483870,485255,485448,485459,485937,485950,486132,486405,486444,486452,486458,486462,486466,486467}; // 1036512
    ppj.selfJoinAndAddRecord(tokens, 1036512, results);
    
    
    
    
    System.out.println(Arrays.toString(results.toArray()));
    // Wenn man zuerst die 1107254 eingibt, dann bekommt man die 1036512 und umgekehrt. Das stimmt auch so: man bekommt einfach immer die Liste der IDs, die zur aktuellen RID ähnlich sind.
    // Wenn man die ersetzen Tokenlisten laufen lässt, funktioniert es nicht, wenn die 54 am Anfang steht. Es muss dann zwingend die 1036512 am Anfang stehen.
    // Das ist aus meiner Sicht eine Optimierung vom PPJoin, der diese Reihenfolge der Records voraussetzt: War das die Unterscheidung zwischen Index- und Präfixlänge??
  }
}
