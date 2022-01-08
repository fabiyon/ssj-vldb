/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package de.huberlin.tokenizer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Pattern;

/**
 *
 * @author fabi
 */
public class VernicaTokenizer extends Tokenizer {

  private static final Pattern rePunctuation = Pattern
          .compile("[^\\p{L}\\p{N}]"); // L:Letter, N:Number
  private static final Pattern reSpaceOrAnderscore = Pattern
          .compile("(_|\\s)+");

  @Override
  public String[] split(String input) {
    String intermediate = clean(input);
    List<String> tokenList = tokenize(intermediate);
    return (String[])tokenList.toArray(new String[0]);
  }

  public String clean(String in) {
    /*
     * - remove punctuation
     * - normalize case
     * - remove extra spaces
     * - replace space with token separator underscore _
     */

    in = rePunctuation.matcher(in).replaceAll(" ");
    in = reSpaceOrAnderscore.matcher(in).replaceAll(" ");
    in = in.trim();
    in = in.replace(' ', '_');
    in = in.toLowerCase();
    return in;
  }

  private List<String> tokenize(String input) {
    final ArrayList<String> returnVect = new ArrayList();
    final HashMap<String, Integer> tokens = new HashMap();
    boolean isFirst = true;
    
    for (String term : input.split("_")) {
      if (term.length() == 0) {
        continue;
      }
      if (isFirst && withRid) {
        isFirst = false;
        returnVect.add(term); // das ist die RID
      }
      Integer count = tokens.get(term);
      if (count == null) {
        count = 0;
      }
      count++;
      tokens.put(term, count);
      returnVect.add(term + "_" + count);
    }
    return returnVect;
  }

}
