/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package de.huberlin.tests;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 *
 * @author fabi
 */
public class CheckIfComplete {
  String inputNotComplete;
  String inputComplete;
  
//  public static void main(String[] args) throws Exception { // just for testing
//    CheckIfComplete cif = new CheckIfComplete("/home/fabi/researchProjects/textualSimilarity/data/output", "/home/fabi/researchProjects/textualSimilarity/data/output");
//  }
  
  public CheckIfComplete(String inputNotComplete, String inputComplete) throws Exception { // we first check if these are folders or files
    this.inputNotComplete = checkIfFolderAndConsolidate(inputNotComplete);
    this.inputComplete = checkIfFolderAndConsolidate(inputComplete);
  }
  
  public String checkIfFolderAndConsolidate(String path) throws Exception {
    File file = new File(path);

    if (!file.exists()) {      // First, make sure the path exists
      throw new Exception("File " + path + " does not exist.");
    }
    
    if (file.isFile()) { // Similarly, this will tell you if it's a file
      return path;
    }
    
    if (file.isDirectory()) { // This will tell you if it is a directory
      // read all contents and consolidate:
      File outFile = new File(file.getCanonicalPath() + File.separator + "consolidated");
//      PrintWriter output = new PrintWriter(outFile);
      FileWriter fstream = new FileWriter(outFile, true);
      BufferedWriter out = new BufferedWriter(fstream);
      
      for (File inFile : file.listFiles()) {
        if (inFile.getName().contains("crc")) {
          continue;
        }
        FileInputStream fis = new FileInputStream(inFile);
        BufferedReader in = new BufferedReader(new InputStreamReader(fis));
        String aLine = null;
        while ((aLine = in.readLine()) != null) {
          //Process each line and add output to destination file
          out.write(aLine);
          out.newLine();
        }
        in.close();
      }
      
      out.close();
      return outFile.getAbsolutePath();
    }
    
    throw new Exception("Unknown file type for " + path);
  }
  
  public void switchInput() {
    String tmp = inputNotComplete;
    inputNotComplete = inputComplete;
    inputComplete = tmp;
  }
  
  public boolean isComplete() {
    BufferedReader br = null;
    boolean isComplete = true;
    try {
      String sCurrentLine;
      br = new BufferedReader(new FileReader(inputNotComplete)); // file to check (maybe not complete)
      HashMap<Integer, ArrayList<Integer>> hm = new HashMap();
      int numberOfMissingPairs = 0;
      
      while ((sCurrentLine = br.readLine()) != null) {
        String[] lineSplit = sCurrentLine.split("\\s+");
        int rid1 = Integer.parseInt(lineSplit[0]);
        int rid2 = Integer.parseInt(lineSplit[1]);
        
        // sort them (smallest first):
        if (rid2 < rid1) {
          int tmp = rid1;
          rid1 = rid2;
          rid2 = tmp;
        }
        
        ArrayList<Integer> currentList; // rid2 list of the current rid1
        if (hm.containsKey(rid1)) {
          currentList = hm.get(rid1);
        } else {
          currentList = new ArrayList();
          hm.put(rid1, currentList);
        }
        currentList.add(rid2);
      }
      
      Iterator<Map.Entry<Integer, ArrayList<Integer>>> it = hm.entrySet().iterator();
      int count = 0;
      while (it.hasNext()) {
        Map.Entry<Integer, ArrayList<Integer>> pairs = (Map.Entry)it.next();
        count += pairs.getValue().size();
      }
      System.out.println("added " + count + " entries.");
      
      br = new BufferedReader(new FileReader(inputComplete)); // complete file
      while ((sCurrentLine = br.readLine()) != null) {
        String[] lineSplit = sCurrentLine.split("\\s+");
        int rid1 = Integer.parseInt(lineSplit[0]);
        int rid2 = Integer.parseInt(lineSplit[1]);

        // sort them (smallest first):
        if (rid2 < rid1) {
          int tmp = rid1;
          rid1 = rid2;
          rid2 = tmp;
        }
        
        // find in hm:
        ArrayList<Integer> currentList = hm.get(rid1);
        if (currentList == null || (currentList != null && !currentList.contains(rid2))) {
          System.out.println("Found missing pair " + rid1 + " " + rid2);
          numberOfMissingPairs++;
          isComplete = false;
        }
      }
      System.out.println("Total number of missing pairs: " + numberOfMissingPairs);
    } catch (IOException e) {
    } finally {
      try {
        if (br != null) {
          br.close();
        }
      } catch (IOException ex) {
      }
    }
    return isComplete;
  }
}
