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
 * @author fabi
 */
public class TokenizeEnron implements Runnable {

  static int nextRecordId = 1;
  static final ArrayList<String> globalTokens = new ArrayList();
  String file;
  int recordId;
  static PrintWriter pw;
  static ArrayBlockingQueue<TokenizeEnron> queue;

  public TokenizeEnron(String file, int recordId) {
    this.file = file;
    this.recordId = recordId;
  }

  @Override
  public void run() {
    Reader freader = null;
    try {
      ArrayList<Integer> tokenBuffer = new ArrayList();
      freader = new FileReader(file);
      ArrayList<String> localTokens = new ArrayList();
      StreamTokenizer tokenizer = new StreamTokenizer(freader);
      tokenizer.lowerCaseMode(true);
      tokenizer.eolIsSignificant(true);
      int successiveEolCount = 0;
      while (tokenizer.nextToken() != StreamTokenizer.TT_EOF) {

        if (tokenizer.ttype == StreamTokenizer.TT_WORD) {
          if (successiveEolCount > 1) { // non-header
            String val = tokenizer.sval.replace(".", "");
            if (val.length() > 1) {
              while (localTokens.contains(val)) {
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
          }
          if (successiveEolCount == 1) {
            successiveEolCount = 0;
          }
        } else if (tokenizer.ttype == StreamTokenizer.TT_NUMBER) {
          if (successiveEolCount == 1) {
            successiveEolCount = 0;
          }
        } else if (tokenizer.ttype == StreamTokenizer.TT_EOL) {
          successiveEolCount++;
        }

      }
      System.out.println("processed " + file);
      
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
          pw.println(recordId + " " + builder.toString());
        }
      }
      freader.close();
    } catch (FileNotFoundException ex) {
      Logger.getLogger(TokenizeEnron.class.getName()).log(Level.SEVERE, null, ex);
    } catch (IOException ex) {
      Logger.getLogger(TokenizeEnron.class.getName()).log(Level.SEVERE, null, ex);
    } finally {
      try {
        freader.close();
      } catch (IOException ex) {
        Logger.getLogger(TokenizeEnron.class.getName()).log(Level.SEVERE, null, ex);
      }
    }
  }

  public static void readAllFiles(String folder) throws IOException, InterruptedException {
    File entryFolder = new File(folder);
    for (File fileEntry : entryFolder.listFiles()) {
      if (fileEntry.isFile()) {
        // Add work to the queue 
        TokenizeEnron te = new TokenizeEnron(fileEntry.getAbsolutePath(), nextRecordId++);
        queue.put(te); // der put darf bloß nicht synchron erfolgen, sonst hat man logischerweise einen Deadlock
        synchronized (queue) {
          // Notify the monitor object that all the threads
          // are waiting on. This will awaken just one to
          // begin processing work from the queue
          queue.notify();
        }
      } else {
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
    int numberOfThreads = 2; // was 24
    if (args.length > 1) {
      inFolder = args[0];
      outFile = args[1];
      numberOfThreads = Integer.parseInt(args[2]);
    }
    queue = new ArrayBlockingQueue(numberOfThreads + 1); // was 10

    for (int i = 0; i < numberOfThreads; i++) {
        // Create and start the worker thread, which will 
      // immediately wait on the monitor (the queue in 
      // this case) to be signaled to begin processing
      TokenizeEnronWorker worker = new TokenizeEnronWorker(queue);
      worker.start();
    }
    
    pw = new PrintWriter(new BufferedWriter(new FileWriter(outFile, false)));
    readAllFiles(inFolder);
    pw.flush();
    pw.close();
  }
}
