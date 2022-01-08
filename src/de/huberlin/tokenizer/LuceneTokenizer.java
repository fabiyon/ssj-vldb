/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package de.huberlin.tokenizer;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

/**
 *
 * @author fabi
 */
public class LuceneTokenizer extends Tokenizer {

  @Override
  public String[] split(String input) {

    Analyzer analyzer = new EnglishAnalyzer();
    TokenStream ts = analyzer.tokenStream("myfield", new StringReader(input));
    CharTermAttribute charTermAttribute = ts.getAttribute(CharTermAttribute.class);
    ArrayList<String> tmpList = new ArrayList();

    try {
      ts.reset(); // Resets this stream to the beginning. (Required)
      while (ts.incrementToken()) {
        tmpList.add(charTermAttribute.toString());
      }
      ts.end();   // Perform end-of-stream operations, e.g. set the final offset.
    } catch (IOException ex) {
      Logger.getLogger(LuceneTokenizer.class.getName()).log(Level.SEVERE, null, ex);
    } finally {
      try {
        ts.close(); // Release resources associated with this stream.
      } catch (IOException ex) {
        Logger.getLogger(LuceneTokenizer.class.getName()).log(Level.SEVERE, null, ex);
      }
    }
    return tmpList.toArray(new String[0]);
  }
  
}
