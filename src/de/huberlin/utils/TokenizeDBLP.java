/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package de.huberlin.utils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Reader;
import java.io.StreamTokenizer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * run with: java -cp
 * /home/fier/textualSimilarity/code/TextualSimilarityHadoop1/dist/TextualSimilarityHadoop1.jar
 * de.huberlin.utils.TokenizeEnron /home/fier/textualSimilarity/data/maildir
 * /home/fier/textualSimilarity/data/enronout
 * 
 * another example: java -cp textualSimilarity/code/TextualSimilarityHadoop1/dist/TextualSimilarityHadoop1.jar de.huberlin.utils.TokenizeDBLP /home/fier/tokenize_in /home/fier/dblp_tokenized
 *
 * @author fabi
 */
public class TokenizeDBLP {

  static int nextRecordId = 1;
  static final ArrayList<String> globalTokens = new ArrayList();
  String file;
  static int recordId;
  static PrintWriter pw;

  public TokenizeDBLP(String file) {
    this.file = file;
  }

  public void run() {
    Reader freader = null;
    try {
      ArrayList<Integer> tokenBuffer = new ArrayList();
      freader = new FileReader(file);
      ArrayList<String> localTokens = new ArrayList();
      StreamTokenizer tokenizer = new StreamTokenizer(freader);
      tokenizer.lowerCaseMode(true);
      tokenizer.eolIsSignificant(true);
      tokenizer.whitespaceChars('/', '/');
      while (tokenizer.nextToken() != StreamTokenizer.TT_EOF) {
        if (tokenizer.ttype == StreamTokenizer.TT_WORD) {
          String val = tokenizer.sval.replace(".", "");
          if (val.length() > 1) {
            while (localTokens.contains(val)) { // es soll immer ein neuer Wert angenommen werden, wenn er mehrfach auftritt. Das scheint aber problematisch zu sein... <<<<<<<<<<<<<<<<<<<<<
              val += "1";
            }
            localTokens.add(val);

            int tokenInt;
            synchronized (globalTokens) {
              if (!globalTokens.contains(val)) {
                globalTokens.add(val);
              }

              tokenInt = globalTokens.indexOf(val);
            }
            tokenBuffer.add(tokenInt); // simply to order the tokens
          }
        } else if (tokenizer.ttype == StreamTokenizer.TT_EOL) { // slash wird als EOL gewertet und dann der Rest der Zeile übersprungen
          // write out record:
          writeLine(tokenBuffer);
        }

      }
      System.out.println("processed " + file);
      writeLine(tokenBuffer);
      
      freader.close();
    } catch (FileNotFoundException ex) {
      Logger.getLogger(TokenizeDBLP.class.getName()).log(Level.SEVERE, null, ex);
    } catch (IOException ex) {
      Logger.getLogger(TokenizeDBLP.class.getName()).log(Level.SEVERE, null, ex);
    } finally {
      try {
        freader.close();
      } catch (IOException ex) {
        Logger.getLogger(TokenizeDBLP.class.getName()).log(Level.SEVERE, null, ex);
      }
    }
  }
  
  public void writeLine(ArrayList<Integer> tokenBuffer) {
    // prepare buffer:
    Collections.sort(tokenBuffer);
    StringBuilder builder = new StringBuilder();
    if (tokenBuffer.size() > 0) {
      builder.append(tokenBuffer.remove(0));

      for(Integer s : tokenBuffer) {
        builder.append(",");
        builder.append(s);
      }

      synchronized (pw) {
        pw.println(recordId++ + " " + builder.toString());
      }
    }
    tokenBuffer.clear();
  }

  public static void readAllFiles(String folder) throws IOException, InterruptedException {
    File entryFolder = new File(folder);
    for (File fileEntry : entryFolder.listFiles()) {
      if (fileEntry.isFile()) {
        TokenizeDBLP te = new TokenizeDBLP(fileEntry.getAbsolutePath());
        te.run();
      } else { // recursively enter all subdirectories
        readAllFiles(fileEntry.getAbsolutePath());
      }
      synchronized(pw) {
        pw.flush(); // necessary to keep the memory usage low. Otherwise, the program uses up to several hundred MB and can potentially run out of mem.
      }
    }
  }

  public static void main(String[] args) throws FileNotFoundException, IOException, InterruptedException {
    String inFolder = "/home/fabi/researchProjects/textualSimilarity/data/dblp_raw";
    String outFile = "/home/fabi/researchProjects/textualSimilarity/data/dblp_processed";
    if (args.length > 1) {
      inFolder = args[0];
      outFile = args[1];
    }

    pw = new PrintWriter(new BufferedWriter(new FileWriter(outFile, false)));
    readAllFiles(inFolder);
    pw.flush();
    pw.close();
  }
}
