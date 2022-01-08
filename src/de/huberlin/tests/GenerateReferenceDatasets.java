/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package de.huberlin.tests;

import com.google.common.io.Files;
import de.huberlin.vernicajoin.VernicaJob;
import java.io.File;

/**
 *
 * @author fabi
 */
public class GenerateReferenceDatasets {
  public static void main(String[] arg) throws Exception {
    
    
    File file = new File("/home/fabi/researchProjects/textualSimilarity/data/tests/");

    if (!file.exists()) {      // First, make sure the path exists
      return;
    }
    
    if (file.isFile()) { // Similarly, this will tell you if it's a file
      return;
    }
    
    if (file.isDirectory()) { // This will tell you if it is a directory
      for (File inFile : file.listFiles()) {
        String dataset = inFile.getName();
        String[] thresholds = {".9", ".7", ".5"};
        for (String threshold :thresholds) {
          String[] args = {"-t", threshold, "-i", "file:///home/fabi/researchProjects/textualSimilarity/data/tests/" + dataset + "/input", 
            "-o", "file:///home/fabi/researchProjects/textualSimilarity/data/output"};
          VernicaJob.main(args);
          // move output file:
          Files.move(new File("/home/fabi/researchProjects/textualSimilarity/data/output/part-r-00000"), 
                  new File("/home/fabi/researchProjects/textualSimilarity/data/tests/" + dataset + "/output" + threshold));

        }
        
        
      }
    }
  }
}
