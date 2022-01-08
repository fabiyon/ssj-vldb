/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package de.huberlin.tests;

import de.huberlin.textualsimilarityhadoop.ElsayedJob;
import de.huberlin.textualsimilarityhadoop.GroupJoinJob;
import de.huberlin.mgjoin.MGJoinJob;
import de.huberlin.massjoin.Step1SignatureCreationJob;
import de.huberlin.textualsimilarityhadoop.SSJ2RNew;
import de.huberlin.vernicajoin.VernicaJob;
import de.huberlin.textualsimilarityhadoop.VsmartJob;
import java.io.File;
import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;
import org.junit.Test;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;

/**
 *
 * @author fabi
 */
public class JoinTests {
  
  @Test
  public void testOutputOf01() throws Exception {
    String[] algos = {"EJ", "GJ", "MG", "MJ", "SJ", "VS"}; //"VJ",  
    for (String algo : algos) {
      File file = new File("/home/fabi/researchProjects/textualSimilarity/data/tests");
    
      if (file.isDirectory()) { // This will tell you if it is a directory
        for (File inFile : file.listFiles()) {
          String algoName = inFile.getName();
          runTestForDatasetAndThreshold(algo, algoName, ".9");
          runTestForDatasetAndThreshold(algo, algoName, ".7");
          runTestForDatasetAndThreshold(algo, algoName, ".5");
        }
      }
      
    }
  }
  
  private void runTestForDatasetAndThreshold(String algo, String dataset, String threshold) throws Exception {
    String[] args = {"-t", threshold, "-i", "file:///home/fabi/researchProjects/textualSimilarity/data/tests/" + dataset + "/input", 
      "-o", "file:///home/fabi/researchProjects/textualSimilarity/data/output"};
    switch (algo) {
      case "EJ":
        ElsayedJob.main(args);
        break;
      case "GJ":
        GroupJoinJob.main(args);
        break;
      case "MG":
        MGJoinJob.main(args);
        break;
      case "MJ":
        Step1SignatureCreationJob.main(args);
        break;
      case "SJ":
        SSJ2RNew.main(args);
        break;
      case "VJ":
        VernicaJob.main(args);
        break;
      case "VS":
        VsmartJob.main(args);
        break;
    }
    
    CheckIfComplete cic = new CheckIfComplete("/home/fabi/researchProjects/textualSimilarity/data/output", 
            "/home/fabi/researchProjects/textualSimilarity/data/tests/" + dataset + "/output" + threshold);
    assertTrue("Output is not complete for " + algo + " " + dataset + " " + threshold, cic.isComplete());
    cic.switchInput();
    assertTrue("Output contains too many rid pairs for " + algo + " " + dataset + " " + threshold, cic.isComplete());
  }

  public static void main(String[] args) {
    Result result = JUnitCore.runClasses(JoinTests.class);
    for (Failure failure : result.getFailures()) {
      System.out.println(failure.toString());
    }
  }
  
}
