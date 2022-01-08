/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package de.huberlin.tokenizer;

/**
 *
 * @author fabi
 */
public class TokenizerFactory {

  public static Tokenizer getTokenizer(String choice) {
    switch (choice) {
      case "BASIC":
        return new BasicTokenizer();
      case "FS":
        return new FSJoinTokenizer();
      case "LUCENE":
        return new LuceneTokenizer();
      case "VERNICA":
        return new VernicaTokenizer();
    }
    throw new RuntimeException("Unknown tokenizer \"" + choice  + "\".");
  }
}
