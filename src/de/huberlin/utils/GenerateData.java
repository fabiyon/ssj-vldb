/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package de.huberlin.utils;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.math3.distribution.ZipfDistribution;

/**
 *
 * @author fabi
 */
public class GenerateData {
  
  public static void main(String[] args) throws IOException, InterruptedException {
    System.out.println("Usage: java -cp TextualSimilarityHadoop1.jar de.huberlin.utils.GenerateData 100 1 10 .3");
    int numberOfTokens = 1000; // Anzahl der Tokens
    int numberOf100Records = 1; // Anzahl der Records in Hundert
    int maxNumberOfTokensPerRecord = 100;
    double exponent = .5;
    String outFile = "/home/fabi/researchProjects/textualSimilarity/data/zipf_" + numberOfTokens + "_" + (numberOf100Records * 100) + "_" + maxNumberOfTokensPerRecord + "_" + exponent;
    
    if (args.length > 0) {
      numberOfTokens = Integer.parseInt(args[0]);
      numberOf100Records = Integer.parseInt(args[1]);
      maxNumberOfTokensPerRecord = Integer.parseInt(args[2]);
      exponent = Double.parseDouble(args[3]);
      outFile = "./zipf_" + numberOfTokens + "_" + (numberOf100Records * 100) + "_" + maxNumberOfTokensPerRecord + "_" + exponent;
    }
    
    
    
    // Idee: iterative Generierung in 1000er-Blöcken.
    // 0. entscheide über die Länge der 1000 zu generierenden Records:
    ZipfDistribution zd = new ZipfDistribution(maxNumberOfTokensPerRecord, exponent);
    
    final ZipfDistribution zd1 = new ZipfDistribution(numberOfTokens, exponent);
    
    BlockingQueue<String> queue = new ArrayBlockingQueue(100);
    
    int recordId = 1;
//    PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(outFile, true)));
    
    for (int run = 0; run < numberOf100Records; run++) {
      final int[] lengths = zd.sample(100);
      for (int i = 0; i < lengths.length; i++) {
        OneRecord p = new OneRecord(recordId++, lengths[i], zd1, outFile, queue);
        new Thread(p).start();
      }
      // wie wartet man hier...?
//      for (int i = 0; i < queue.size(); i++) {
//        out.println(queue.take());
//      } 
    }
//    out.flush();
//    out.close();
  }
  
  static class OneRecord implements Runnable {
    int recordId;
    int recordLength;
    ZipfDistribution zd;
    String outFile;
    String buffer;
    BlockingQueue queue;
    OneRecord(int recordId, int recordLength,  ZipfDistribution zd, String outFile, BlockingQueue queue) {
      this.recordId = recordId;
      this.recordLength = recordLength;
      this.zd = zd;
      this.outFile = outFile;
//      queue.add("recordId: " + recordId);
      this.queue = queue;
    }
    
    private String arrayToString(int[] arr) {
      String res = "";
      for (int i = 0; i < arr.length; i++) {
        if (!res.isEmpty()) {
          res += ",";
        }
        res += arr[i];
      }
      return res;
    }

    @Override
    public void run() {
      int[] recordTokens = zd.sample(recordLength);
//      try {
//        queue.put(recordId + " " + arrayToString(recordTokens));
        PrintWriter out = null;
        try {

          out = new PrintWriter(new BufferedWriter(new FileWriter(outFile, true)));
        } catch (IOException ex) {
          Logger.getLogger(GenerateData.class.getName()).log(Level.SEVERE, null, ex);
        }
        out.println(recordId + " " + arrayToString(recordTokens));
        out.flush();
        out.close();
//      } catch (InterruptedException ex) {
//        Logger.getLogger(GenerateData.class.getName()).log(Level.SEVERE, null, ex);
//      }
    }
  }
}
