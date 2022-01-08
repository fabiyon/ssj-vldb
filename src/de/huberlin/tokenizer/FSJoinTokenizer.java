/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package de.huberlin.tokenizer;

import java.util.ArrayList;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 * @author fabi
 */
public class FSJoinTokenizer extends Tokenizer {

  @Override
  public String[] split(String input) {
    input = FSJoinTokenizer.TransformString2(input);
    ArrayList<String> tokenList = FSJoinTokenizer.SplitToTokens_Word(input);
    String[] outArr = new String[tokenList.size()];
    outArr = tokenList.toArray(outArr);
    return outArr;
  }

  //transform
  public static String TransformString2(String str) {
    String first = null;
    String stopwords = "\\b(I|a|about|an|and|also|are|as|at|be|by|com|for|from|how|in|is|it|of|on|or|that|the|this|not|to|was|what|when|where|who|will|with|The|www)\\b";
    Pattern p2 = Pattern.compile(stopwords, Pattern.CASE_INSENSITIVE);
    Matcher m2 = p2.matcher(str);
    first = m2.replaceAll(" ");
    // System.out.println("punct: " + first);
    Pattern p = Pattern.compile("[^a-zA-Z ]", Pattern.CASE_INSENSITIVE);// ���Ӷ�Ӧ�ı��
    Matcher m = p.matcher(first);
    first = m.replaceAll(" ");
    // ==================remove over===============
    p = Pattern.compile(" {2,}");// ȥ�����ո�
    m = p.matcher(first);
    String second = m.replaceAll(" ").toLowerCase();
    return second;
  }

  //tokenization
  public static ArrayList<String> SplitToTokens_Word(String content) {
    ArrayList<String> list = new ArrayList<String>();
    StringTokenizer tokens = new StringTokenizer(content);
    while (tokens.hasMoreTokens()) {
      String token = tokens.nextToken();
      if (list.indexOf(token) == -1) {
        list.add(token);
      }
    }
    return list;
  }

}
