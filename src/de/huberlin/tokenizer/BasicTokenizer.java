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
public class BasicTokenizer extends Tokenizer {

  @Override
  public String[] split(String input) {
    return input.split("\\W+");
  }
  
}
