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
import java.util.Comparator;
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
 * java -cp /home/fier/textualSimilarity/code/TextualSimilarityHadoop1/dist/TextualSimilarityHadoop1.jar de.huberlin.utils.ExtractDataForExcel /home/fier/textualSimilarity/data > results.csv
 */
public class ExtractDataForExcel {
  private static String mainPath = "/home/fabi/researchProjects/textualSimilarity/data/tmp";
  
  
  public static void main(String[] args) throws IOException, Exception {
    if (args.length == 1) {
      mainPath = args[0];
    }
    
    // ================ Generate graphs for output ====================
    processOutputFiles();
  }
  
  private static class Experiment {
    public String algorithm;
    public String dataset;
    public float similarity;
    public int dNumberOfRecords;
    public int dNumberOfTokens;
    public float dSkewnessFactor;
    public int dMaxSizeRecords;
    private final File folder;
    private ArrayList<Integer> totalRuntimeSeconds;
    private final TreeMap<String, String> statistics = new TreeMap();
    private static Experiment exampleExperiment; // we need this for the CSV header which is called statically
    
    public Experiment(File folder) throws Exception {
      this.folder = folder;
      exampleExperiment = this;
      statistics.put("terminated", "");
      statistics.put("number of results", "0");
      resetPhases();
      totalRuntimeSeconds = new ArrayList();
      totalRuntimeSeconds.add(getTimeDifference(folder.getAbsolutePath() + "/utilization"));
      readFromFolderName();
      getRuntimeStatistics();
//      getIntermediateResultStatisticsNew(); // diese Funktion ist nur für Statistiken
    }
    
    @Override
    public boolean equals(Object o) {
      if (o == null) {
        return false;
      }
      Experiment oe = (Experiment)o;
      return this.algorithm.equals(oe.algorithm) &&
              this.dataset.equals(oe.dataset) &&
              this.similarity == oe.similarity;
    }
    
    public void resetPhases() {
      for (int phaseNumber = 1; phaseNumber <= 8; phaseNumber++) {
        statistics.put("Phase " + phaseNumber, "");
      }
    }
    
    public void setAdditionalTotalRuntime(int runtime) {
      totalRuntimeSeconds.add(runtime);
    }
    
    public int getTotalRuntime() {
      if (totalRuntimeSeconds == null) {
        return 0;
      }
      int sum = 0;
      for (Integer i : totalRuntimeSeconds) {
        sum += i;
      }
      return sum / totalRuntimeSeconds.size();
    }
    
    public void readFromFolderName() throws Exception {
      String folderName = folder.getName()
              .replaceAll("clusterjoin.", "")
              .replaceAll("massjoin.", "")
              .replaceAll("mgjoin.", "")
              .replaceAll("vernicajoin.", "")
              .replaceAll("vsmart.", "")
              .replaceAll("Driver", "")
              .replaceAll("01-dblp-titleonly-vernicatokenized-", "")
              .replaceAll("-onlytokens", "")
              .replaceAll(".4k.", ".4,3.")
              .replaceAll(".21k.", ".21,5.")
              .replaceAll(".43k.", ".43.")
              .replaceAll(".215k.", ".215.")
              .replaceAll(".430k.", ".430.");
      String[] fileNameArr = folderName.split("\\.");
      if (folderName.contains("-tokenized.")) {
        // 0 output.
        // 1 MassJoinJob.
        // 2 01-dblp-tokenized.
        // 3 2016-01-21-10-22-38.
        // 4 9.
        // 5 .
        algorithm = fileNameArr[1];
        switch (algorithm) {
          case "MGJoinJob":
            algorithm = "MG";
            break;
          case "GroupJoinJob":
            algorithm = "GJ";
            break;
          case "VernicaJob":
            algorithm = "VJ";
            break;
          case "MassJoinJob":
            algorithm = "MJ";
            break;
          case "ElsayedJob":
            algorithm = "EJ";
            break;
          case "VsmartJob":
            algorithm = "VS";
            break;
          case "SSJ2RNew":
            algorithm = "SJ";
            break;
          case "FSJoinJob":
            algorithm = "FS";
            break;
        }

        dataset = fileNameArr[2];
        similarity = Float.parseFloat("." + fileNameArr[4]);
//        
//        String[] zipfParams0 = fileNameArr[2].split("_");
//        dNumberOfRecords  = Integer.parseInt(zipfParams0[0]);
//        dNumberOfTokens  = Integer.parseInt(zipfParams0[1]);
//        
//        String[] zipfParams1 = fileNameArr[3].split("_");
//        dSkewnessFactor  = Float.parseFloat("." + zipfParams1[0]);
//        dMaxSizeRecords  = Integer.parseInt(zipfParams1[1]);
      } else if (folderName.contains("output.")) {
        // 0 output.
        // 1 MassJoinJob.
        // 2 10000_100_.  // ${NUMBER_OF_RECORDS}_${DICT_SIZE}_
        // 3 7_100. // ${ZIPF_VALUE}_${REC_LENGTH}
        // 4 2016-01-21-10-22-38.
        // 5 9.
        // 6 .
        algorithm = fileNameArr[1];
        int add = 0;
        switch (algorithm) {
          case "ClusterJoinJob":
            algorithm = "CJ";
            break;
          case "FSJoinJob":
            algorithm = "FS";
            break;
          case "MGJoinJob":
          case "mgjoin":
            add++;
            algorithm = "MG";
            break;
          case "GroupJoinJob":
            algorithm = "GJ";
            break;
          case "VernicaJob":
          case "vernicajoin":
            add++;
            algorithm = "VJ";
            break;
          case "MassJoinJob":
          case "massjoin":
            add++;
            algorithm = "MJ";
            break;
          case "ElsayedJob":
            algorithm = "EJ";
            break;
          case "VsmartJob":
            algorithm = "VS";
            break;
          case "SSJ2RNew":
            algorithm = "SJ";
            break;
        }
        
        
        dataset = fileNameArr[2 + add];// + "" + fileNameArr[3 + add];
        if (fileNameArr[2 + add + 1].contains("converted")) {
          dataset += fileNameArr[2+add+1].replace("converted", "");
        }
        
        // nimm den letzten nicht-leeren Eintrag im Array
        String tmp = "";
        for (int i = fileNameArr.length - 1; i > 1; i--) {
          if (!fileNameArr[i].isEmpty()) {
            similarity = Float.parseFloat("." + fileNameArr[i]);
            break;
          }
        }
        
//        String[] zipfParams0 = fileNameArr[2].split("_");
//        dNumberOfRecords  = Integer.parseInt(zipfParams0[0]);
//        dNumberOfTokens  = Integer.parseInt(zipfParams0[1]);
//        
//        String[] zipfParams1 = fileNameArr[3].split("_");
//        dSkewnessFactor  = Float.parseFloat("." + zipfParams1[0]);
//        dMaxSizeRecords  = Integer.parseInt(zipfParams1[1]);
      } else {
        throw new Exception("not yet implemented");
//        algoName = fileNameArr[1];
//        inputName = fileNameArr[2];
//        threshold = Integer.parseInt(fileNameArr[4]);
      }
    }
    
//    public void getTotalRuntime() throws Exception {
//      
//    }
    
    public void getRuntimeStatistics() throws FileNotFoundException, IOException {
      for (File fileEntry : folder.listFiles()) {
        if (fileEntry.isFile()) {
          if (fileEntry.getName().contains(".xml")) {
            // read XML
//            try {
//              DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
//              DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
//              Document doc = dBuilder.parse(fileEntry);
//              //optional, but recommended
//              //read this - http://stackoverflow.com/questions/13786607/normalization-in-dom-parsing-with-java-how-does-it-work
//              doc.getDocumentElement().normalize(); // nimmt unnötige Zeilenumbrüche raus
//              NodeList nList = doc.getElementsByTagName("property");
//              for (int temp = 0; temp < nList.getLength(); temp++) {
//                Node nNode = nList.item(temp);
//                if (nNode.getNodeType() == Node.ELEMENT_NODE) {
//                  Element eElement = (Element) nNode;
//                  switch(eElement.getElementsByTagName("name").item(0).getTextContent()) {
//                    case "dfs.replication":
//                    case "dfs.block.size":
//                    case "mapreduce.map.memory.mb":
//                    case "mapreduce.reduce.memory.mb":
//                    case "mapreduce.map.java.opts":
//                    case "mapreduce.reduce.java.opts":
//                    case "mapreduce.job.maps":
//                    case "mapreduce.job.reduces":
//                      hadoopConf.put(eElement.getElementsByTagName("name").item(0).getTextContent(), eElement.getElementsByTagName("value").item(0).getTextContent());
//                      break;
//                  }
//                }
//              }
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
          } else if (fileEntry.getName().equals("slaves") || fileEntry.getName().equals("consolidatedResults")) {
            InputStream is = new BufferedInputStream(new FileInputStream(fileEntry.getAbsoluteFile()));
            try {
                byte[] c = new byte[1024];
                int count = 0;
                int readChars = 0;
                while ((readChars = is.read(c)) != -1) {
                    for (int i = 0; i < readChars; ++i) {
                        if (c[i] == '\n') {
                            ++count;
                        }
                    }
                }
                if (fileEntry.getName().equals("slaves")) {
                  statistics.put("number of nodes", count + "");
                } else {
                  statistics.put("number of results", count + "");
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

            for (int i = 1; i < dateList.size(); i++) {
              Date nextDate = dateList.get(i);
              long differenceInSeconds = (nextDate.getTime() - lastDate.getTime()) / 1000;
              statistics.put("Phase " + phaseNumber++, differenceInSeconds + "");
              lastDate = nextDate;
            }

          } else if (fileEntry.getName().contains("terminated")) {
            statistics.put("terminated", "TERM");
          }
        }
      }
    }
    
    public void getIntermediateResultStatisticsNew() {
      // Öffne die Datei consolidatedResults in folder, generiere die Statistik (schreibe key-values in statistics)
      BufferedReader br;
      try {
        br = new BufferedReader(new FileReader(folder.getAbsolutePath() + "/consolidatedResults"));
        String sCurrentLine;
        String currentNode = "";
        int currentCount = 0;
        ArrayList<Integer> numberOfRecordsPerNode = new ArrayList();
        while ((sCurrentLine = br.readLine()) != null) {
          String[] tmpArr = sCurrentLine.split(" "); // numberOfPartitions + " " + totalNumberOfRecords + " " + maxNumberOfRecords + " " + minNumberOfRecords
//          if (tmpArr[0].equals(currentNode)) {
//            currentCount += Integer.parseInt(tmpArr[1]);
//          } else {
//            if (currentCount != 0) {
//              numberOfRecordsPerNode.add(currentCount);
//            }
//            currentCount = 0;
//            currentNode = tmpArr[0];
//          }
          numberOfRecordsPerNode.add(Integer.parseInt(tmpArr[1]));
        }
//        if (currentCount != 0) {
//          numberOfRecordsPerNode.add(currentCount);
//        }
     
        // min, max, mean of records:
        int minNumberOfRecords = Integer.MAX_VALUE;
        int maxNumberOfRecords = Integer.MIN_VALUE;
        int totalNumberOfRecords = 0;
        for (Integer i : numberOfRecordsPerNode) {
          if (minNumberOfRecords > i) {
            minNumberOfRecords = i;
          }
          if (maxNumberOfRecords < i) {
            maxNumberOfRecords = i;
          }
          totalNumberOfRecords += i;
        }
        int avgNumberOfRecords = totalNumberOfRecords / numberOfRecordsPerNode.size();
        
        statistics.put("minNumberOfRecords", minNumberOfRecords + ""); // pro Knoten
        statistics.put("maxNumberOfRecords", maxNumberOfRecords + "");
        statistics.put("avgNumberOfRecords", avgNumberOfRecords + "");
        statistics.put("totalNumberOfRecords", totalNumberOfRecords + "");
      } catch (Exception e) {
        System.out.println(e);
      }
    }
    
    public void getIntermediateResultStatistics() {
      // Öffne die Datei consolidatedResults in folder, generiere die Statistik (schreibe key-values in statistics)
      BufferedReader br;
      try {
        br = new BufferedReader(new FileReader(folder.getAbsolutePath() + "/consolidatedResults"));
        String sCurrentLine;
        String lastNode = "";
        ArrayList<Integer> numberOfPartitionsPerNode = new ArrayList();
        ArrayList<Integer> numberOfRecordsPerNode = new ArrayList();
        Integer currentNumberOfPartitions = 0;
        Integer currentNumberOfRecords = 0;
        while ((sCurrentLine = br.readLine()) != null) {
          String[] tmpArr = sCurrentLine.split(" ");
          if (lastNode.equals(tmpArr[0])) {
            currentNumberOfPartitions++;
            currentNumberOfRecords += Integer.parseInt(tmpArr[1]);
          } else {
            if (currentNumberOfPartitions != 0) {
              numberOfPartitionsPerNode.add(currentNumberOfPartitions);
            }
            if (currentNumberOfRecords != 0) {
              numberOfRecordsPerNode.add(currentNumberOfRecords);
            }
            currentNumberOfPartitions = new Integer(1);
            currentNumberOfRecords = Integer.parseInt(tmpArr[1]);;
            lastNode = tmpArr[0];
          }
        }
        if (currentNumberOfPartitions != 0) {
          numberOfPartitionsPerNode.add(currentNumberOfPartitions);
        }
        if (currentNumberOfRecords != 0) {
          numberOfRecordsPerNode.add(currentNumberOfRecords);
        }
        
        // min, max, mean of partitions:
        int minNumberOfPartitions = Integer.MAX_VALUE;
        int maxNumberOfPartitions = Integer.MIN_VALUE;
        int totalNumberOfPartitions = 0;
        for (Integer i : numberOfPartitionsPerNode) {
          if (minNumberOfPartitions > i) {
            minNumberOfPartitions = i;
          }
          if (maxNumberOfPartitions < i) {
            maxNumberOfPartitions = i;
          }
          totalNumberOfPartitions += i;
        }
        int avgNumberOfPartitions = totalNumberOfPartitions / numberOfPartitionsPerNode.size();
        
        statistics.put("minNumberOfPartitions", minNumberOfPartitions + "");
        statistics.put("maxNumberOfPartitions", maxNumberOfPartitions + "");
        statistics.put("avgNumberOfPartitions", avgNumberOfPartitions + "");
        
        // min, max, mean of records:
        int minNumberOfRecords = Integer.MAX_VALUE;
        int maxNumberOfRecords = Integer.MIN_VALUE;
        int totalNumberOfRecords = 0;
        for (Integer i : numberOfRecordsPerNode) {
          if (minNumberOfRecords > i) {
            minNumberOfRecords = i;
          }
          if (maxNumberOfRecords < i) {
            maxNumberOfRecords = i;
          }
          totalNumberOfRecords += i;
        }
        int avgNumberOfRecords = totalNumberOfRecords / numberOfRecordsPerNode.size();
        
        statistics.put("minNumberOfRecords", minNumberOfRecords + "");
        statistics.put("maxNumberOfRecords", maxNumberOfRecords + "");
        statistics.put("avgNumberOfRecords", avgNumberOfRecords + "");
      } catch (Exception e) {
        System.out.println(e);
      }
    }
    
    public static String getCSVHeader() {
      String res =  "algorithm;"
        + "dataset;"
        + "similarity;"
        + "dNumberOfRecords;"
        + "dNumberOfTokens;"
        + "dSkewnessFactor;"
        + "dMaxSizeRecord;"
        + "totalRuntimeSeconds;";
      for(Map.Entry<String,String> entry : exampleExperiment.statistics.entrySet()) {
        res +=  entry.getKey() + ";";
      }
      return res;
    }
    
    public String getCSV() {
      if (!statistics.get("terminated").isEmpty()) {
        resetPhases(); // delete the runtimes, because they only confuse
      }
      String res =  algorithm + ";"
        + dataset + ";"
        + similarity + ";"
        + dNumberOfRecords + ";"
        + dNumberOfTokens + ";"
        + dSkewnessFactor + ";"
        + dMaxSizeRecords + ";"
        + this.getTotalRuntime() + ";";
      for(Map.Entry<String,String> entry : statistics.entrySet()) {
        res +=  entry.getValue() + ";";
      }
      return res;
    }
    
    @Override
    public String toString() {
      return "algorithm: " + algorithm + "\n"
        + "dataset: " + dataset + "\n"
        + "similarity: " + similarity + "\n"
        + "dNumberOfRecords: " + dNumberOfRecords + "\n"
        + "dNumberOfTokens: " + dNumberOfTokens + "\n"
        + "dSkewnessFactor: " + dSkewnessFactor + "\n"
        + "dMaxSizeRecords: " + dMaxSizeRecords + "\n"
        + "totalRuntimeSeconds: " + this.getTotalRuntime();
    }
  }
  
//  private static class DataSet {
//    public String datasetName;
//    public int lines;
//    public int tokens;
//    public int minTokens;
//    public int maxTokens;
//    public long fileSize;
//    
//    public DataSet(String datasetName, int lines, int tokens, int minTokens, int maxTokens, long fileSize) {
//      this.datasetName = datasetName;
//      this.lines = lines;
//      this.tokens = tokens;
//      this.minTokens = minTokens;
//      this.maxTokens = maxTokens;
//      this.fileSize = fileSize;
//    }
//    
//    public String toLatex() {
//      return datasetName.replace("_", "\\_") + " & " + lines + " & " + tokens + " & " + minTokens + " & " + maxTokens + " & " + fileSize +  " \\\\ \\hline";
//    }
//  }
  

//  
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
        if (lineSplit.length == 2) {
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

//      datasets.put(inputName, new DataSet(inputName, lineNumber, tokenFrequencies.size(), minTokens, maxTokens, getFileSizeOf(inputPath)));
      
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
  
  private static int getTimeDifference(String inputFile) throws Exception {
    BufferedReader br;
//    TreeMap<Long, Integer> utilization = new TreeMap();
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
//        Date tmpDate = ft.parse(sCurrentLine);
        String[] tmpLineArr = sCurrentLine.split("\\s+");
//        if (tmpLineArr.length >= 3) { // <<<<<<<<<<<<<<<<<<<<<<<<< monitorUtilization liefert hier plötzlich Unfug <<<<<<<<<<<<<<<
//          utilization.put(tmpDate.getTime(), Integer.parseInt(tmpLineArr[2]));
//        }
      }
      if (firstLine == null) {
        System.out.println("Problem in file " + inputFile);
        return Integer.MAX_VALUE;
      }
      Date start = ft.parse(firstLine);
      Date end = ft.parse(lastLine);
      
      timeDifference = (int)((end.getTime() - start.getTime()) / 1000); // getTime() retrieves milliseconds. I wanna get seconds.
      
//      int numberOfTicks = utilization.size();
//      if (numberOfTicks > 50) {
//        numberOfTicks = 50;
//      }
//      double divisor = Math.ceil((double)(end.getTime() - start.getTime() + 1) / (double)numberOfTicks);
//      int[] results = new int[numberOfTicks];
//      for(Map.Entry<Long,Integer> entry : utilization.entrySet()) {
//        int partition = (int)Math.floor((entry.getKey() - start.getTime()) / divisor);
//        if (partition > numberOfTicks - 1) {
//          System.out.println("Berechnungsproblem Partition");
//        } else {
//          results[partition] = entry.getValue(); // we just take the last entry for this partition and omit all other ones between for simplicity. A mean would also be possible, but not very meaningful probably.
//        }
//      }
      
      // save @ plotOutput/2
//      File plotOutputPath2 = new File(mainPath + "/plotOutput/2");
//      plotOutputPath2.mkdirs();
//      String outFile2 = plotOutputPath2.getAbsoluteFile() + "/" + experimentName;
//      PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(outFile2, false)));
//      for (int i = 0; i < results.length; i++) {
//        String fromTo = (int)((i * divisor)/1000) + "-" + (int)(((i + 1) * divisor) / 1000);
//        out.println(i + " " + fromTo + " " + results[i]);
//      }
//      out.flush();
//      out.close();
      
      // PLOT
//      pdfs.add(outFile2 + ".pdf");
//      String[] command2 = {"/usr/bin/gnuplot", 
//                "-e", 
//                "filename='" + outFile2 + "'; titlename='" + experimentName + "'; outputname='" + outFile2 + ".pdf'",
//                "plot_output2.gnuplot"};
//      runCommand(command2, mainPath, null, false);
      
      
    } catch (IOException e) {
      System.out.println(e);
    } catch (ParseException ex) {
      Logger.getLogger(ExtractDataForExcel.class.getName()).log(Level.SEVERE, null, ex);
    } finally {
    }
    return timeDifference;
  }
  
//  public static void readHadoopConfigurationAndRunStatistics(File folder) throws FileNotFoundException, IOException {
//    for (File fileEntry : folder.listFiles()) {
//      if (fileEntry.isFile()) {
//        if (fileEntry.getName().contains(".xml")) {
//          // read XML
//          try {
//            DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
//            DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
//            Document doc = dBuilder.parse(fileEntry);
//            //optional, but recommended
//            //read this - http://stackoverflow.com/questions/13786607/normalization-in-dom-parsing-with-java-how-does-it-work
//            doc.getDocumentElement().normalize(); // nimmt unnötige Zeilenumbrüche raus
//            NodeList nList = doc.getElementsByTagName("property");
//            for (int temp = 0; temp < nList.getLength(); temp++) {
//              Node nNode = nList.item(temp);
//              if (nNode.getNodeType() == Node.ELEMENT_NODE) {
//                Element eElement = (Element) nNode;
//                switch(eElement.getElementsByTagName("name").item(0).getTextContent()) {
//                  case "dfs.replication":
//                  case "dfs.block.size":
//                  case "mapreduce.map.memory.mb":
//                  case "mapreduce.reduce.memory.mb":
//                  case "mapreduce.map.java.opts":
//                  case "mapreduce.reduce.java.opts":
//                  case "mapreduce.job.maps":
//                  case "mapreduce.job.reduces":
//                    hadoopConf.put(eElement.getElementsByTagName("name").item(0).getTextContent(), eElement.getElementsByTagName("value").item(0).getTextContent());
//                    break;
//                }
//              }
//            }
//          } catch (Exception e) {
//              e.printStackTrace();
//          }
//        } else if (fileEntry.getName().equals("slaves") || fileEntry.getName().equals("consolidatedResults")) {
//          InputStream is = new BufferedInputStream(new FileInputStream(fileEntry.getAbsoluteFile()));
//          try {
//              byte[] c = new byte[1024];
//              int count = 0;
//              int readChars = 0;
//              boolean empty = true;
//              while ((readChars = is.read(c)) != -1) {
//                  empty = false;
//                  for (int i = 0; i < readChars; ++i) {
//                      if (c[i] == '\n') {
//                          ++count;
//                      }
//                  }
//              }
//              if (fileEntry.getName().equals("slaves")) {
////                hadoopConf.put("number of nodes", count + "");
//              } else {
//                TreeMap tmp = runStatistics.get(folder.getName());
//                if (tmp == null) {
//                  tmp = new TreeMap();
//                  runStatistics.put(folder.getName(), tmp);
//                }
//                tmp.put("number of results", count + "");
//              }
//          } finally {
//              is.close();
//          }
//        } else if (fileEntry.getName().equals("hadoop-stdout")) {
//          BufferedReader br = new BufferedReader(new FileReader(fileEntry.getAbsoluteFile()));
//          String line;
//          boolean alreadyShowedOneException = false;
//          Date lastDate = null;
//          ArrayList<Date> dateList = new ArrayList();
//          SimpleDateFormat ft = new SimpleDateFormat("y/M/d H:m:s");
//          
//          while((line = br.readLine()) != null) {
//            if (line.contains("Exception") && !alreadyShowedOneException) {
//              System.out.println("EXCEPTION WARNING: " + fileEntry.getAbsoluteFile());
//              alreadyShowedOneException = true;
//            }
//
//            try {
//              lastDate = ft.parse(line);
//              if (line.contains("client.RMProxy: Connecting to ResourceManager")) {
//                dateList.add(lastDate);
//              }
//            } catch (ParseException ex) { // ignore, that's fine
////              Logger.getLogger(PlotEverything.class.getName()).log(Level.SEVERE, null, ex);
//            }
//          }
//          dateList.add(lastDate);
//          // process dateList:
//          int phaseNumber = 1;
//          lastDate = dateList.get(0);
//          TreeMap tmp = runStatistics.get(folder.getName());
//          if (tmp == null) {
//            tmp = new TreeMap();
//            runStatistics.put(folder.getName(), tmp);
//          }
//          for (int i = 1; i < dateList.size(); i++) {
//            Date nextDate = dateList.get(i);
//            long differenceInSeconds = (nextDate.getTime() - lastDate.getTime()) / 1000;
//            tmp.put("Phase " + phaseNumber++, differenceInSeconds + "s");
//            lastDate = nextDate;
//          }
//          
//        }
//      }
//    }
//  }
//  
  public static void processOutputFiles() throws IOException, Exception {
    ArrayList<Experiment> experiments = new ArrayList();
    File folder = new File(mainPath);// + "/output");
   
    Experiment lastExperiment = null;
    Experiment currentExperiment = null;
    ArrayList<String> fileNames = new ArrayList();
    for (File fileEntry : folder.listFiles()) {
      if (fileEntry.isDirectory()) {
        fileNames.add(fileEntry.getName());
      }
    }
    Collections.sort(fileNames);
    
    for (String fileName : fileNames) {
      fileName = folder.getAbsolutePath() + "/" + fileName;
      File fileEntry = new File(fileName);
      if (fileEntry.isDirectory()) {
        currentExperiment = new Experiment(fileEntry);
        if (lastExperiment != null && currentExperiment.equals(lastExperiment)) { // only add total runtime
          lastExperiment.setAdditionalTotalRuntime(currentExperiment.getTotalRuntime());
        } else { // add new experiment:
          experiments.add(currentExperiment);
        }
        lastExperiment = currentExperiment;
      }
    }
    
    Collections.sort(experiments, new Comparator<Experiment>() {
      @Override
      public int compare(Experiment a, Experiment b) {
        return a.algorithm.compareTo(b.algorithm);
      }
    });
    System.out.println(Experiment.getCSVHeader());
    for (Experiment e : experiments) {
      System.out.println(e.getCSV());
    }
  }
  
}
