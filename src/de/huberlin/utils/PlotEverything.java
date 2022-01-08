/*
 * Generates input statistics, input histograms, and output performance graphs
 * Prerequisites: a main folder (configure it with mainPath in main class) with the following subfolders:
 * 1. input: all input files
 * 2. output: all output folders
 */

package de.huberlin.utils;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 *
 * @author fabi
 */
public class PlotEverything {
  // Die Daten für die Eingabestatistiken müssen in data/input liegen:
  private static final boolean regenerateInputStatistics = false; // <<<<<<<<<<<<<<<< we can skip this if we don't add new input files!
  private static final ArrayList<String> pdfs = new ArrayList();
  private static final ArrayList<String> histograms = new ArrayList();
  private static final TreeMap<String, DataSet> datasets = new TreeMap();
  // Die Ausgabedaten müssen unter data/output liegen:
  private static final String mainPath = "/home/fabi/researchProjects/textualSimilarity/data/resultplots/resultsOfHadoop";
  private static final TreeMap<String, String> hadoopConf = new TreeMap();
  private static final TreeMap<String, TreeMap<String, String>> runStatistics = new TreeMap(); // name of run (=folder name), {<key, value>}
  
  public static void main(String[] args) throws IOException {
    // ================ Generate statistics and histograms for input ====================
    processInputFiles();

    // ================ Generate graphs for output ====================
    processOutputFiles();
    processHadoopConfigAndOutputStatistics();
    
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
  
  private static void processHadoopConfigAndOutputStatistics() throws IOException {
    File outputTablePath = new File(mainPath + "/outputTables");
    outputTablePath.mkdir();
    String outTableFilename = outputTablePath + "/outputTables.tex";
    String outPdfFilename = outputTablePath + "/outputTables.pdf";
    
    PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(outTableFilename, false)));
    out.println("\\documentclass[10pt,a4paper,oneside,ngerman]{article}\n" +
      "\\usepackage{graphicx}\n" +
      "\\usepackage{balance}\n" +
      "\\usepackage{subfig}\n " + 
      "\\usepackage{longtable}\n" + 
      "\\usepackage[margin=0.1in]{geometry}\n" +
      "\\begin{document}" +
      "\n" +
      "\\begin{table*}[ht]\n" +
      "\\centering\n" +
      "\\caption{Hadoop configuration.}\n" +
      "\\begin{tabular}{|c|c|} \\hline\nname 	& value 	\\\\ \\hline\n");
    for(Map.Entry<String,String> entry : hadoopConf.entrySet()) { // we receive this automatically sorted alphabetically
      out.println(entry.getKey() + " & " + entry.getValue() +  " \\\\ \\hline");
    }
    out.println("\\end{tabular}\n" +
      "\\end{table*}");
    
    out.println(
//            "\\begin{table*}[ht]\n" +
//      "\\centering\n" +
//      "\\caption{Experiment statistics.}\n" +
      "\\begin{longtable}{|c|c|c|} \\hline\nexperiment & name 	& value 	\\\\ \\hline\n");
    for(Map.Entry<String,TreeMap<String, String>> entry : runStatistics.entrySet()) { // we receive this automatically sorted alphabetically
      String experimentName = entry.getKey().replace("_", "\\_");
      TreeMap<String, String> expStatistics = entry.getValue();
      for(Map.Entry<String, String> innerEntry : expStatistics.entrySet()) {
        out.println(experimentName + " & " + innerEntry.getKey() + " & " + innerEntry.getValue() +  " \\\\ \\hline");
      }
    }
    out.println("\\end{longtable}\n" 
//     + "\\end{table*}"
    );
    
    
    out.println("\\end{document}");
    out.flush();
    out.close();
    
    // we must delete the old PDF due to a bug in pdftk: when re-applying pdflatex on an existing pdf, pdftk produces a weird error when combining the pdfs.
    File f = new File(outPdfFilename);
    f.delete();
    
    String[] command = {"pdflatex",
      "outputTables.tex"};
    runCommand(command, outputTablePath.getAbsolutePath(), null, true); // this command only works if we capture the debug output
    pdfs.add(outPdfFilename);
  }
  
  private static void processInputFiles() throws IOException {
    if (regenerateInputStatistics) {
      File histogramPath = new File(mainPath + "/histograms/recordlengths");
      histogramPath.mkdirs();
      File tokenFrequencyPath = new File(mainPath + "/histograms/tokenfrequencies");
      tokenFrequencyPath.mkdir();

      File folder = new File(mainPath + "/input");
      for (File fileEntry : folder.listFiles()) {
        if (!fileEntry.isDirectory()) {
          String fileName = fileEntry.getName();
          String[] fileNameArr = fileName.split("\\.");

          String inputName = fileNameArr[0];

          String histogramFile =      histogramPath.getAbsolutePath() + "/" + fileNameArr[0];
          String tokenHistogramFile = tokenFrequencyPath.getAbsolutePath() + "/" + fileNameArr[0];
          generateStatisticFor(fileEntry.getAbsolutePath(), histogramFile, tokenHistogramFile, inputName); // input file, output file (for histogram)

          histograms.add(histogramFile + ".pdf");
          histograms.add(tokenHistogramFile + ".pdf");
          String[] command = {"/usr/bin/gnuplot", 
                  "-e", 
                  "filename='" + histogramFile + "'; titlename='" + fileNameArr[0] + "'; outputname='" + histogramFile + ".pdf'",
                  "plot_histograms.gnuplot"};
          runCommand(command, mainPath, null, false); 

          String[] command1 = {"/usr/bin/gnuplot", 
                  "-e", 
                  "filename='" + tokenHistogramFile + "'; titlename='" + fileNameArr[0] + "'; outputname='" + tokenHistogramFile + ".pdf'",
                  "plot_token_histograms.gnuplot"};
          runCommand(command1, mainPath, null, true); 
        }
      }

      PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(mainPath + "/dataTable/dataTable1.tex", false)));
      out.println("\\documentclass[10pt,a4paper,oneside,ngerman]{article}\n" +
        "\\usepackage{graphicx}\n" +
        "\\usepackage{balance}\n" +
        "\\usepackage{subfig}\n \\usepackage[margin=0.1in]{geometry}" +
        "\\begin{document}" +
        "\n" +
        "\\begin{table*}[ht]\n" +
        "\\centering\n" +
        "\\caption{Input datasets.}\n" +
        "\\begin{tabular}{|c|c|c|p{3cm}|c|p{2cm}|} \\hline\nDataset 	& number of lines 	& number of tokens 	& min tokens 		& max tokens 		& file size (kB) 	\\\\ \\hline\n");
      for(Map.Entry<String,DataSet> entry : datasets.entrySet()) { // we receive this automatically sorted alphabetically
        out.println(entry.getValue().toLatex());
      }
      out.println("\\end{tabular}\n" +
        "\\end{table*} \n \\begin{figure}\n" +
        "\\centering");

      int count = 0;
      for (String histogram : histograms) {
        out.println("\\subfloat{\n" +
          "\\includegraphics[width=0.35\\linewidth]{" + histogram + "}}");
        if (count % 2 == 1) {
          out.println("\\\\[1pt]");
        }
        count++;
      }
      out.println("\\end{figure} \n \\end{document}");
      out.flush();
      out.close();

      // we must delete the old PDF due to a bug in pdftk: when re-applying pdflatex on an existing pdf, pdftk produces a weird error when combining the pdfs.
      File f = new File(mainPath + "/dataTable" + "/dataTable1.pdf");
      f.delete();

      String[] command = {"pdflatex",
        "dataTable1.tex"};
      runCommand(command, mainPath + "/dataTable", null, true); // this command only works if we capture the debug output
    }
    pdfs.add(mainPath + "/dataTable/dataTable1.pdf");
  }
  
  // pdfs must have absolute paths!
  private static void consolidatePdfs() throws IOException {
    Collections.sort(pdfs);
    String[] command1 = new String[4 + pdfs.size()];
    command1[0] = "pdftk";
    int i = 0;
    for (; i < pdfs.size(); i++) {
      command1[i+1] = pdfs.get(i);
    }
    command1[++i] = "cat";
    command1[++i] = "output";
    command1[++i] = "results.pdf";
    runCommand(command1, mainPath, null, false);
  }
  
  private static void generateStatisticFor(String inputPath, String outputPath, String tokenHistogramFile, String inputName) {
    // we want to plot the number of tokens on the x axis and the number or records
    // containing this number of tokens on the y axis.
    
    BufferedReader br;
    try {
      
      br = new BufferedReader(new FileReader(inputPath));
      
      String sCurrentLine;
      HashMap<Integer, ArrayList<Integer>> hm = new HashMap();
      int lineNumber = 0;

      TreeMap<Integer, Integer> tokenFrequencies = new TreeMap();
      int minTokens = Integer.MAX_VALUE;
      int maxTokens = 0;
      TreeMap<Integer, Integer> histogram = new TreeMap(); // numberOfTokens => numberOfRecords
      
      while ((sCurrentLine = br.readLine()) != null) {
        String[] lineSplit = sCurrentLine.split("\\s+");
        String[] tokens;
        if (lineSplit.length >= 2) {
          tokens = lineSplit[1].split(",");
        } else {
          tokens = new String[0];
        }
        
        int lineLength = tokens.length;
        if (minTokens > lineLength) {
          minTokens = lineLength;
        }
        if (maxTokens < lineLength) {
          maxTokens = lineLength;
        }
        for (String s : tokens) {
          int token = Integer.parseInt(s);
          Integer frequency = tokenFrequencies.get(token);
          if (frequency == null) {
            frequency = 1;
          } else {
            frequency++;
          }
          tokenFrequencies.put(token, frequency);
        }
        Integer numberOfRecords = histogram.get(lineLength);
        if (numberOfRecords == null) {
          numberOfRecords = 1;
        } else {
          numberOfRecords++;
        }
        histogram.put(lineLength, numberOfRecords);
        lineNumber++;
      }

      datasets.put(inputName, new DataSet(inputName, lineNumber, tokenFrequencies.size(), minTokens, maxTokens, getFileSizeOf(inputPath)));
      
//      int count = 0;
//      for(Map.Entry<Integer,Integer> entry : histogram.entrySet()) {
//        System.out.println("Exact histogram: " + entry.getKey() + " " + entry.getValue());
//        if (count++ > 10) {
//          break;
//        }
//      }
      
      // Consolidated histogram: group the numberOfTokens into equal-sized pieces so that we have 10 partitions in the end:
      int frequencyInOnePartition = (maxTokens - minTokens) / 20;
      TreeMap<Integer, Integer> consolidatedHistogram = new TreeMap();
      for(Map.Entry<Integer,Integer> entry : histogram.entrySet()) {
        int currentFrequency = entry.getKey();
        int currentNumberOfRecords = entry.getValue();
        int partition = (currentFrequency - minTokens) / frequencyInOnePartition;
        
        Integer consolidatedNumberOfRecords = consolidatedHistogram.get(partition);
        if (consolidatedNumberOfRecords == null) {
          consolidatedNumberOfRecords = currentNumberOfRecords;
        } else {
          consolidatedNumberOfRecords += currentNumberOfRecords;
        }
        consolidatedHistogram.put(partition, consolidatedNumberOfRecords);
      }
//      System.out.println("Consolidated histogram");
      PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(outputPath, false)));
      out.println("partitionId lowerBound-upperBound numberOfRecords");
      for(Map.Entry<Integer,Integer> entry : consolidatedHistogram.entrySet()) {
        int partitionNumber = entry.getKey();
        int lowerBound = partitionNumber * frequencyInOnePartition + minTokens;
        int upperBound = lowerBound + frequencyInOnePartition - 1;
        out.println(partitionNumber + " " + lowerBound + "-" + upperBound + " " + entry.getValue());
      }
      out.flush();
      out.close();
      
      // compute the token frequencies:
      TreeMap<Integer, Integer> inverseTokenFrequencies = new TreeMap();
      for (Map.Entry<Integer, Integer> entry : tokenFrequencies.entrySet()) {
        int frequency = entry.getValue();
        Integer numberOfTokensWithThisFrequency = inverseTokenFrequencies.get(frequency);
        if (numberOfTokensWithThisFrequency == null) {
          numberOfTokensWithThisFrequency = 1;
        } else {
          numberOfTokensWithThisFrequency++;
        }
        inverseTokenFrequencies.put(frequency, numberOfTokensWithThisFrequency);
      }
      
      Integer[] consolidatedInvTokFreqs = new Integer[10];
      double largestFreq = inverseTokenFrequencies.lastEntry().getKey();
      int divisor = (int)Math.ceil((largestFreq + 1) / 10.0);
      for (Map.Entry<Integer, Integer> entry : inverseTokenFrequencies.entrySet()) {
        int frequency = entry.getKey();
        int numberOfTokensWithThisFrequency = entry.getValue();
        int partition = (int)Math.floor(frequency / divisor);
        Integer currentNumberOfTokens = consolidatedInvTokFreqs[partition];
        if (currentNumberOfTokens == null) {
          currentNumberOfTokens = numberOfTokensWithThisFrequency;
        } else {
          currentNumberOfTokens += numberOfTokensWithThisFrequency;
        }
        consolidatedInvTokFreqs[partition] = currentNumberOfTokens;
      }
      
      PrintWriter out1 = new PrintWriter(new BufferedWriter(new FileWriter(tokenHistogramFile, false)));
      out1.println("lineNumber frequency numberOfTokensWithThisFrequency");
      int lineNum = 0;
      for (int partition = 0; partition < 10; partition++) {
        String fromTo = (partition * divisor) + "-" + (((partition + 1) * divisor) - 1);
        Integer numberOfTokensWithThisFreq = consolidatedInvTokFreqs[partition];
        if (numberOfTokensWithThisFreq == null) {
          numberOfTokensWithThisFreq = 0;
        }
        out1.println(lineNum++ + " " + fromTo + " " + numberOfTokensWithThisFreq);
//        System.out.println("frequency: " + frequency + ", numberOfTokensWithThisFrequency: " + numberOfTokensWithThisFrequency);
      }
      out1.flush();
      out1.close();
      
    } catch (IOException e) {
      System.out.println(e);
    } finally {
    }
  }
  
  private static long getFileSizeOf (String path) {
    File f = new File(path);
    return f.length() / 1024;
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
  
  private static int getTimeDifference(String inputFile, String experimentName) {
    BufferedReader br;
    TreeMap<Long, Integer> utilization = new TreeMap();
    int timeDifference = 0;
    SimpleDateFormat ft = new SimpleDateFormat("M/d/y H:m:s");
    try {
      br = new BufferedReader(new FileReader(inputFile));
      String sCurrentLine;
      String firstLine = null;
      String lastLine = null;
      while ((sCurrentLine = br.readLine()) != null) {
        if (firstLine == null) {
          firstLine = sCurrentLine;
        }
        lastLine = sCurrentLine;
        Date tmpDate = ft.parse(sCurrentLine);
        String[] tmpLineArr = sCurrentLine.split("\\s+");
        if (tmpLineArr.length >= 3) { // <<<<<<<<<<<<<<<<<<<<<<<<< monitorUtilization liefert hier plötzlich Unfug <<<<<<<<<<<<<<<
          utilization.put(tmpDate.getTime(), Integer.parseInt(tmpLineArr[2]));
        }
      }
      if (firstLine == null) {
        System.out.println("Problem in file " + inputFile);
        return Integer.MAX_VALUE;
      }
      Date start = ft.parse(firstLine);
      Date end = ft.parse(lastLine);
      
      timeDifference = (int)((end.getTime() - start.getTime()) / 1000); // getTime() retrieves milliseconds. I wanna get seconds.
      
      int numberOfTicks = utilization.size();
      if (numberOfTicks > 50) {
        numberOfTicks = 50;
      }
      double divisor = Math.ceil((double)(end.getTime() - start.getTime() + 1) / (double)numberOfTicks);
      int[] results = new int[numberOfTicks];
      for(Map.Entry<Long,Integer> entry : utilization.entrySet()) {
        int partition = (int)Math.floor((entry.getKey() - start.getTime()) / divisor);
        if (partition > numberOfTicks - 1) {
          System.out.println("Berechnungsproblem Partition");
        } else {
          results[partition] = entry.getValue(); // we just take the last entry for this partition and omit all other ones between for simplicity. A mean would also be possible, but not very meaningful probably.
        }
      }
      
      // save @ plotOutput/2
      File plotOutputPath2 = new File(mainPath + "/plotOutput/2");
      plotOutputPath2.mkdirs();
      String outFile2 = plotOutputPath2.getAbsoluteFile() + "/" + experimentName;
      PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(outFile2, false)));
      for (int i = 0; i < results.length; i++) {
        String fromTo = (int)((i * divisor)/1000) + "-" + (int)(((i + 1) * divisor) / 1000);
        out.println(i + " " + fromTo + " " + results[i]);
      }
      out.flush();
      out.close();
      
      // PLOT
      pdfs.add(outFile2 + ".pdf");
      String[] command2 = {"/usr/bin/gnuplot", 
                "-e", 
                "filename='" + outFile2 + "'; titlename='" + experimentName + "'; outputname='" + outFile2 + ".pdf'",
                "plot_output2.gnuplot"};
      runCommand(command2, mainPath, null, false);
      
      
    } catch (IOException e) {
      System.out.println(e);
    } catch (ParseException ex) {
      Logger.getLogger(PlotEverything.class.getName()).log(Level.SEVERE, null, ex);
    } finally {
    }
    return timeDifference;
  }
  
  public static void readHadoopConfigurationAndRunStatistics(File folder) throws FileNotFoundException, IOException {
    for (File fileEntry : folder.listFiles()) {
      if (fileEntry.isFile()) {
        if (fileEntry.getName().contains(".xml")) {
          // read XML
          try {
            DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
            Document doc = dBuilder.parse(fileEntry);
            //optional, but recommended
            //read this - http://stackoverflow.com/questions/13786607/normalization-in-dom-parsing-with-java-how-does-it-work
            doc.getDocumentElement().normalize(); // nimmt unnötige Zeilenumbrüche raus
            NodeList nList = doc.getElementsByTagName("property");
            for (int temp = 0; temp < nList.getLength(); temp++) {
              Node nNode = nList.item(temp);
              if (nNode.getNodeType() == Node.ELEMENT_NODE) {
                Element eElement = (Element) nNode;
                switch(eElement.getElementsByTagName("name").item(0).getTextContent()) {
                  case "dfs.replication":
                  case "dfs.block.size":
                  case "mapreduce.map.memory.mb":
                  case "mapreduce.reduce.memory.mb":
                  case "mapreduce.map.java.opts":
                  case "mapreduce.reduce.java.opts":
                  case "mapreduce.job.maps":
                  case "mapreduce.job.reduces":
                    hadoopConf.put(eElement.getElementsByTagName("name").item(0).getTextContent(), eElement.getElementsByTagName("value").item(0).getTextContent());
                    break;
                }
              }
            }
          } catch (Exception e) {
              e.printStackTrace();
          }
        } else if (fileEntry.getName().equals("slaves") || fileEntry.getName().equals("consolidatedResults")) {
          InputStream is = new BufferedInputStream(new FileInputStream(fileEntry.getAbsoluteFile()));
          try {
              byte[] c = new byte[1024];
              int count = 0;
              int readChars = 0;
              boolean empty = true;
              while ((readChars = is.read(c)) != -1) {
                  empty = false;
                  for (int i = 0; i < readChars; ++i) {
                      if (c[i] == '\n') {
                          ++count;
                      }
                  }
              }
              if (fileEntry.getName().equals("slaves")) {
                hadoopConf.put("number of nodes", count + "");
              } else {
                TreeMap tmp = runStatistics.get(folder.getName());
                if (tmp == null) {
                  tmp = new TreeMap();
                  runStatistics.put(folder.getName(), tmp);
                }
                tmp.put("number of results", count + "");
              }
          } finally {
              is.close();
          }
        } else if (fileEntry.getName().equals("hadoop-stdout")) {
          BufferedReader br = new BufferedReader(new FileReader(fileEntry.getAbsoluteFile()));
          String line;
          boolean alreadyShowedOneException = false;
          Date lastDate = null;
          ArrayList<Date> dateList = new ArrayList();
          SimpleDateFormat ft = new SimpleDateFormat("y/M/d H:m:s");
          
          while((line = br.readLine()) != null) {
            if (line.contains("Exception") && !alreadyShowedOneException) {
              System.out.println("EXCEPTION WARNING: " + fileEntry.getAbsoluteFile());
              alreadyShowedOneException = true;
            }

            try {
              lastDate = ft.parse(line);
              if (line.contains("client.RMProxy: Connecting to ResourceManager")) {
                dateList.add(lastDate);
              }
            } catch (ParseException ex) { // ignore, that's fine
//              Logger.getLogger(PlotEverything.class.getName()).log(Level.SEVERE, null, ex);
            }
          }
          dateList.add(lastDate);
          // process dateList:
          int phaseNumber = 1;
          lastDate = dateList.get(0);
          TreeMap tmp = runStatistics.get(folder.getName());
          if (tmp == null) {
            tmp = new TreeMap();
            runStatistics.put(folder.getName(), tmp);
          }
          for (int i = 1; i < dateList.size(); i++) {
            Date nextDate = dateList.get(i);
            long differenceInSeconds = (nextDate.getTime() - lastDate.getTime()) / 1000;
            tmp.put("Phase " + phaseNumber++, differenceInSeconds + "s");
            lastDate = nextDate;
          }
          
        }
      }
    }
  }
  
  public static void processOutputFiles() throws IOException {
    HashMap<String, HashMap<Integer, TreeMap<String, Integer>>> results = new HashMap(); // inputName, threshold, AlgoName, timeDifference
    File folder = new File(mainPath + "/output");
    File plotOutputPath0 = new File(mainPath + "/plotOutput/0"); // 0: one graph/ file per input, x=algorithm, grouped by threshold
    plotOutputPath0.mkdirs();
    File plotOutputPath1 = new File(mainPath + "/plotOutput/1"); // 1: one graph/ file per input, x=threshold, grouped by algorithm
    plotOutputPath1.mkdirs();
    
    String outputFolder = plotOutputPath0.getAbsolutePath() + "/";
    HashSet<String> fullAlgoList = new HashSet();
    for (File fileEntry : folder.listFiles()) {
      if (fileEntry.isDirectory()) {
        readHadoopConfigurationAndRunStatistics(fileEntry);
        
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
        

        // Start- und Endzeit:
        int timeDifference = getTimeDifference(fileEntry.getAbsolutePath() + "/utilization", fileName);
//        System.out.println("timeDifference: " + timeDifference);
        
        HashMap<Integer, TreeMap<String, Integer>> c1 = null; // threshold, ...
        if (!results.containsKey(inputName)) {
          c1 = new HashMap();
          results.put(inputName, c1);
        } else {
          c1 = results.get(inputName);
        }
        TreeMap<String, Integer> c2 = null;
        if (!c1.containsKey(threshold)) {
          c2 = new TreeMap();
          c1.put(threshold, c2);
        } else {
          c2 = c1.get(threshold);
        }
        c2.put(algoName, timeDifference);
        fullAlgoList.add(algoName);
      }
    }
    
    String header;
    String buffer;
    ArrayList<String> pdfs1 = new ArrayList();
    for(Map.Entry<String, HashMap<Integer, TreeMap<String, Integer>>> entry : results.entrySet()) {
      String inputName = entry.getKey();
      HashMap<Integer, TreeMap<String, Integer>> value = entry.getValue();
      String outFile = outputFolder + inputName + ".inp";
      PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(outFile, false)));
      
      header = "Algorithm\t";
      
      buffer = "";
      
      for (Map.Entry<Integer, TreeMap<String, Integer>> entry1 : value.entrySet()) { // threshold => TreeMap<Algo, runtime>
        int threshold = entry1.getKey();
        buffer += "." + threshold + "\t";
        TreeMap<String, Integer> value1 = entry1.getValue();
        // add missing values for algorithms if not present:
        for (String algoName : fullAlgoList) {
          if (!value1.containsKey(algoName)) {
            value1.put(algoName, 0);
          }
        }
        
        for (Map.Entry<String, Integer> entry2 : value1.entrySet()) { // problem: das geht davon aus, dass alle Algorithmen mit allen Schwellwerten ausgeführt wurden...
          String algoName = entry2.getKey();
          if (!header.contains(algoName)) {
            header += algoName + "\t";
          }
          
          Integer timeDifference = entry2.getValue();
          
          // manually remove outliers:
          if (algoName.equals("MassJoinJob") && timeDifference > 6000 && outFile.contains("input_20000")) {
            timeDifference = 6000;
          }
          buffer += timeDifference + "\t";
        }
        buffer += "\n";
      }
      
      out.print(header + "\n" + buffer);
      
      out.flush();
      out.close();
      
      pdfs.add(outFile.replace(".inp", ".pdf"));
      String[] command0 = {"/usr/bin/gnuplot", 
                "-e", 
                "filename='" + outFile + "'; titlename='" + inputName + "'; outputname='" + outFile.replace(".inp", ".pdf") + "'",
                "plot_output0.gnuplot"};
      runCommand(command0, mainPath, null, true);
      
      // transpose: 
      // /usr/bin/awk -f transpose.awk input_5000.inp > outputPath/1/...
      String[] command1 = {"/usr/bin/awk",
        "-f",
        "transpose.awk",
        outFile};
      // gnuplot -e "filename='histograms/dblp_5000'" -e "titlename='dblp_5000'" -e "outputname='histograms/dblp_5000.pdf'" ../../plot_output1.gnuplot
      String outFile1 = mainPath + "/plotOutput/1/" + inputName + ".inp";
      runCommand(command1, mainPath, new File(outFile1), false);
      
      pdfs1.add(outFile1.replace(".inp", ".pdf"));
      String[] command2 = {"/usr/bin/gnuplot", 
                "-e", 
                "filename='" + outFile1 + "'; titlename='" + inputName + "'; outputname='" + outFile1.replace(".inp", ".pdf") + "'",
                "plot_output1.gnuplot"};
      runCommand(command2, mainPath, null, false);
      
      pdfs.addAll(pdfs1);
    }
  }
  
}
