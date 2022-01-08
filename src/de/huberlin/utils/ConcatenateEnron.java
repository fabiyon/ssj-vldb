/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package de.huberlin.utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
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
public class ConcatenateEnron implements Runnable {

  static int nextRecordId = 1;
  static final ArrayList<String> globalTokens = new ArrayList();
  String file;
  int recordId;
  static PrintWriter pw;
  static ArrayBlockingQueue<ConcatenateEnron> queue;

  public ConcatenateEnron(String file, int recordId) {
    this.file = file;
    this.recordId = recordId;
  }

  @Override
  public void run() {
    StringBuffer buffer = new StringBuffer();
    try (BufferedReader br = new BufferedReader(new FileReader(file))) {
      String line;
      boolean isFirstCall = true;
      while ((line = br.readLine()) != null) {
        if (!isFirstCall) {
          buffer.append(" ");
        } else {
          isFirstCall = false;
        }
        buffer.append(line);
      }
    } catch (FileNotFoundException ex) {
      Logger.getLogger(ConcatenateEnron.class.getName()).log(Level.SEVERE, null, ex);
    } catch (IOException ex) {
      Logger.getLogger(ConcatenateEnron.class.getName()).log(Level.SEVERE, null, ex);
    } finally {
      
    }
    pw.println(buffer);
  }

  public static void readAllFiles(String folder) throws IOException, InterruptedException {
    File entryFolder = new File(folder);
    for (File fileEntry : entryFolder.listFiles()) {
      if (fileEntry.isFile()) {
        // Add work to the queue 
        ConcatenateEnron te = new ConcatenateEnron(fileEntry.getAbsolutePath(), nextRecordId++);
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

  /**
   * java -cp textualSimilarity/code/TextualSimilarityHadoop1/dist/TextualSimilarityHadoop1.jar de.huberlin.utils.ConcatenateEnron /home/fier/maildir /home/fier/enron_raw 10
   * (dauert unter einer Minute für die vollen 1,4 GB)
   * @param args
   * @throws FileNotFoundException
   * @throws IOException
   * @throws InterruptedException 
   */
  public static void main(String[] args) throws FileNotFoundException, IOException, InterruptedException {
    String inFolder = "/home/fabi/researchProjects/textualSimilarity/data/maildirenron";
    String outFile = "/home/fabi/researchProjects/textualSimilarity/data/enron_raw";
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
    System.exit(0);
  }
}
