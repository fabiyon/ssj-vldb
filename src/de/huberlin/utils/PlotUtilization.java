/*
 * Generates input statistics, input histograms, and output performance graphs
 * Prerequisites: a main folder (configure it with mainPath in main class) with the following subfolders:
 * 1. input: all input files
 * 2. output: all output folders
 */

package de.huberlin.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

/**
 *
 * @author fabi
 */
public class PlotUtilization {
  private static final ArrayList<String> utilizations = new ArrayList();
  // Die Ausgabedaten müssen unter data/output liegen:
  private static final String mainPath = "/home/fabi/researchProjects/textualSimilarity/data/resultplots/resultsOfHadoop";
  
  public static void main(String[] args) throws IOException {
    // ================ Generate graphs for output ====================
    processOutputFiles();
    
    // ================ Consolidate all generated PDFs ===============
    consolidatePdfs();
  }
  
  private static class DataSet {
    public String datasetName;
    public int lines;
    public int tokens;
    public int minTokens;
    public int maxTokens;
    public long fileSize;
    
    public DataSet(String datasetName, int lines, int tokens, int minTokens, int maxTokens, long fileSize) {
      this.datasetName = datasetName;
      this.lines = lines;
      this.tokens = tokens;
      this.minTokens = minTokens;
      this.maxTokens = maxTokens;
      this.fileSize = fileSize;
    }
    
    public String toLatex() {
      return datasetName.replace("_", "\\_") + " & " + lines + " & " + tokens + " & " + minTokens + " & " + maxTokens + " & " + fileSize +  " \\\\ \\hline";
    }
  }
  

  // pdfs must have absolute paths!
  private static void consolidatePdfs() throws IOException {
    Collections.sort(utilizations);
    String[] command1 = new String[4 + utilizations.size()];
    command1[0] = "pdftk";
    int i = 0;
    for (; i < utilizations.size(); i++) {
      command1[i+1] = utilizations.get(i);
    }
    command1[++i] = "cat";
    command1[++i] = "output";
    command1[++i] = "results.pdf";
    runCommand(command1, mainPath, null, false);
  }
  

  
  public static void runCommand(String[] command, String mainPath, File outputFile, boolean doDebug) throws IOException {
    ProcessBuilder pb = new ProcessBuilder(command);
    pb.directory(new File(mainPath));
    if (outputFile != null) {
      pb.redirectOutput(outputFile);
    }
    Process process = pb.start();
        
    if (doDebug) {
      System.out.println(Arrays.toString(command));
      InputStream is = process.getErrorStream();
      InputStreamReader isr = new InputStreamReader(is);
      BufferedReader br = new BufferedReader(isr);
      String line;
      while ((line = br.readLine()) != null) {
        System.out.println(line);
      }
    }
  }
  

  public static void processOutputFiles() throws IOException {
    File folder = new File(mainPath + "/output");
    for (File fileEntry : folder.listFiles()) {
      if (fileEntry.isDirectory()) {
        String fileName = fileEntry.getName();
        fileName = fileName.replaceFirst("clusterjoin.ClusterJoinDriver", "CJ");
        fileName = fileName.replaceFirst("ElsayedJob", "FF");
        fileName = fileName.replaceFirst("GroupJoinJob", "GJ");
        fileName = fileName.replaceFirst("massjoin.MassJoinDriver", "MJ");
        fileName = fileName.replaceFirst("mgjoin.MGJoinDriver", "MG");
        fileName = fileName.replaceFirst("MRSimJoinJob", "MR");
        fileName = fileName.replaceFirst("SSJ2RNew", "S2");
        fileName = fileName.replaceFirst("vernicajoin.VernicaJoinDriver", "VJ");
        fileName = fileName.replaceFirst("vsmart.VsmartJoinDriver", "VS");
        fileName = fileName.replaceFirst("ClusterJoin", "CJ");
        fileName = fileName.replaceFirst("FullFiltering", "FF");
        fileName = fileName.replaceFirst("MassJoin", "MJ");
        fileName = fileName.replaceFirst("MGJoin", "MG");
        fileName = fileName.replaceFirst("VernicaJoin", "VJ");
        fileName = fileName.replaceFirst("VsmartJoin", "VS");
        
        fileName = fileName.replaceFirst(".converted", "");
        
        
        String[] fileNameArr = fileName.split("\\.");

        String algoName;
        String inputName;
        Integer threshold;
        if (fileName.contains(".out")) {
          algoName = fileNameArr[1];
          inputName = fileNameArr[2] + "" + fileNameArr[3];
          threshold = Integer.parseInt(fileNameArr[6]);
        } else {
          algoName = fileNameArr[1];
          inputName = fileNameArr[2];
          threshold = Integer.parseInt(fileNameArr[4]);
        }
        
        // PLOT
        String experimentName = algoName + "-" + inputName + "-" + threshold;
        String outFile2 = "output/" + fileEntry.getName() + "/" + experimentName + ".pdf";
        utilizations.add(outFile2);
        String[] command2 = {"/usr/bin/gnuplot", 
                  "-e", 
                  "filename='output/" + fileEntry.getName() + "/utilization'; titlename='" + experimentName + "'; outputname='" + outFile2 + "'",
                  "plot_output2a.gnuplot"};
        runCommand(command2, mainPath, null, true);
      }
    }
  }
  
}
