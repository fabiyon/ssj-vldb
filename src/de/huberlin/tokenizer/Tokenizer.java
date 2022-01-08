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
public abstract class Tokenizer {
  boolean withRid = false;
  
  public void setWithRid(boolean withRid) {
    this.withRid = withRid;
  }
  
  public abstract String[] split(String input);
}
