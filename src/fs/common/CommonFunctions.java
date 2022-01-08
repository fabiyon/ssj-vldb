package fs.common;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.StringTokenizer;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.StringUtils;

public class CommonFunctions {

  //transform
  public static String TransformString2(String str) { // brauchen wir nicht, wird aber überall verwendet, daher trivialerweise:
    return str;
  }

  //tonkenization
  public static ArrayList<String> SplitToTokens_Word(String content) {
    ArrayList<String> list = new ArrayList<String>();
    StringTokenizer tokens = new StringTokenizer(content, ","); // Worte werden nur durch Kommata getrennt
    while (tokens.hasMoreTokens()) {
      String token = tokens.nextToken();
      if (Configure.PermitDuplicatesInSet == false) {
        if (list.indexOf(token) == -1) {
          list.add(token);
        }
      } else {
        list.add(token);
      }
    }
    return list;
  }

  public static long LoadToAllNodesMap(Configuration conf,
          HashMap<String, Integer> GlobalOrdering, Vector<SortTokens> TF) throws IOException {
    int SequenceNumber = 0;
    StringTokenizer tokens;
    String gram;
    String temp;
    int weight;
    long TotalFQ = 0;
    File prefixFile = new File("./dict");
    try (BufferedReader br = new BufferedReader(new FileReader(prefixFile))) {
      String line;
      while ((line = br.readLine()) != null) {
        if (line.length() == 0) {
          continue;
        }
        SequenceNumber++;
        tokens = new StringTokenizer(line);
        if (tokens.hasMoreTokens()) {
          gram = tokens.nextToken();
          temp = tokens.nextToken();
          weight = Integer.parseInt(temp);
          TotalFQ += weight;
          GlobalOrdering.put(gram, SequenceNumber);
          SortTokens st = new SortTokens(gram, weight);
//            System.err.println("add to TF: gram=" + gram + ", weight=" + weight);
          TF.add(st);
        }
      }
    } catch (Exception e) {
      System.err.println("Error reading dict file! " + e);
    }
    return TotalFQ;
  }

  public static long LoadToOneNodeMap(Configuration conf,
          HashMap<String, Integer> GlobalOrdering, Vector<SortTokens> TF)
          throws IOException {
    String line = null;
    int SequenceNumber = 0;
    StringTokenizer tokens = null;
    String gram = null;
    String temp = null;
    int weight = 0;
    long TotalFQ = 0;
    URI[] myuri = new URI[2];
    try {
      myuri = DistributedCache.getCacheFiles(conf);
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    for (URI ui : myuri) {
      System.err.println("Map cache path is: " + ui.toString());
      System.err.println("Map cache path Number is: " + myuri.length);
      String pathtmp = ui.toString();
      if (pathtmp.indexOf("dict") != -1) {
        BufferedReader fis = new BufferedReader(new FileReader(ui
                .toString()));
        while ((line = fis.readLine()) != null) {
          if (line.length() == 0) {
            continue;
          }
          SequenceNumber++;
          tokens = new StringTokenizer(line);
          if (tokens.hasMoreTokens()) {
            gram = tokens.nextToken();
            temp = tokens.nextToken();
            weight = Integer.parseInt(temp);
            TotalFQ += weight;
            GlobalOrdering.put(gram, SequenceNumber);
            SortTokens st = new SortTokens(gram, weight);
            TF.add(st);
          }
        }
        break;
      }
    }
    return TotalFQ;
  }

  public static long LoadToOneNodeReduce(Configuration conf,
          Vector<ListElement> toSort, HashMap<Integer, Integer> Rid2Sequence) throws IOException {
    String line = null;
    int listNumber = 0;
    StringTokenizer tokens = null;
    URI[] myuri = new URI[2];
    try {
      myuri = DistributedCache.getCacheFiles(conf);
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    for (URI ui : myuri) {
      System.err.println("Reduece cache path is: " + ui.toString());
      System.err.println("Reduece cache path Number is: " + myuri.length);
      String pathtmp = ui.toString();
      if (pathtmp.indexOf("prefix") != -1) {
        BufferedReader bufread = new BufferedReader(new FileReader(ui
                .toString()));
        StringBuffer ioBuffer = new StringBuffer();
        char[] charBuffer = new char[4 * 1024];
        int rlen = bufread.read(charBuffer);
        ioBuffer.append(charBuffer);
        while (rlen != -1) {
          char[] charBuffer2 = new char[4 * 1024];
          rlen = bufread.read(charBuffer2);
          ioBuffer.append(charBuffer2);
          //System.out.println(charBuffer2);
        }
        //System.out.println(charBuffer);//TODO:			
        String[] lines = ioBuffer.toString().split("\n");
        //while((strLine = bufread.readLine())!=null)
        for (String strLine : lines) {
          if (strLine.trim().length() == 0) {
            continue;
          }
          listNumber++;
          //System.out.println("line: "+strLine);
          String[] splits = strLine.split("\t");
          //get token id and length
          String[] RIDAndLength = splits[0].split(" ");
          //System.out.println(RIDAndLength[0]+" "+RIDAndLength[1]+":"+splits[1]);					
          int RID = Integer.parseInt(RIDAndLength[0]);
          int Length = Integer.parseInt(RIDAndLength[1]);

          String[] prefix = splits[1].split(" ");
          int prefixSize = prefix.length;
          int[] prefixTokens = new int[prefixSize];
          for (int i = 0; i < prefixSize; i++) {
            prefixTokens[i] = Integer.parseInt(prefix[i]);
          }
          ListElement forSort = new ListElement(RID, Length, prefixTokens);
          toSort.add(forSort);
          //get Prefix				
        }
        ioBuffer.setLength(0);
        bufread.close();
        //Collections.sort(toSort);
        System.out.println("Setup: Load Prefix Index End");
      }
    }
    return listNumber;
  }

  public static long LoadToAllNodesReduce(Configuration conf,
          Vector<ListElement> toSort, HashMap<Integer, Integer> buYong) throws IOException {
    int sequenceNumber = 0;
    File prefixFile = new File("./prefix");
    try (BufferedReader br = new BufferedReader(new FileReader(prefixFile))) {
      String strLine;
      while ((strLine = br.readLine()) != null) {
        if (strLine.trim().length() == 0) {
          continue;
        }
        sequenceNumber++;
        String[] splits = strLine.split("\t");
        String[] RIDAndLength = splits[0].split(" ");					
        int RID = Integer.parseInt(RIDAndLength[0]);
        int Length = Integer.parseInt(RIDAndLength[1]);
        String[] prefix = splits[1].split(" ");
        int prefixSize = prefix.length;
        int[] prefixTokens = new int[prefixSize];
        for (int i = 0; i < prefixSize; i++) {
          prefixTokens[i] = Integer.parseInt(prefix[i]);
        }
        ListElement forSort = new ListElement(RID, Length, prefixTokens);
        toSort.add(forSort);
      }
    } catch (Exception e) {
      System.err.println("Error reading prefix file! " + e);
    }
    return sequenceNumber;
  }
  
  //probe Number >index number  prefix lenth in prefix is probe number
  public static int getProbeIndex(int length, double similarityThreshold) {
    return length - (int) Math.ceil((double) length * similarityThreshold) + 1;
  }

  public static int getIndexPrefixLength(int length, double similarityThreshold) {
    float simThr100 = (float) (similarityThreshold * 100);
    return length
            - (int) Math.ceil(2 * simThr100 / (100 + simThr100) * length)
            + 1;
  }

  public static boolean passPositionFilter(int noGramsCommon, int positionX,
          int lengthX, int positionY, int lengthY, double similarityThreshold) {
    return getIntersectUpperBound(noGramsCommon, positionX, positionY,
            lengthX, lengthY) >= getIntersectLowerBound(lengthX, lengthY, similarityThreshold);
  }

  public static int getIntersectUpperBound(int noGramsCommon, int positionX,
          int positionY, int lengthX, int lengthY) {
    return noGramsCommon
            + Math.min(lengthX - positionX - 1, lengthY - positionY - 1);
  }

  public static int getIntersectLowerBound(int lengthX, int lengthY, double similarityThreshold) {
    float simThr100 = (float) (similarityThreshold * 100.0);
    return (int) Math.ceil(simThr100 * (lengthX + lengthY)
            / (100 + simThr100));
  }

  public static int CommonUpperBound(Partition p1, Partition p2) {
    int comm = Math.min(p1.getPrev(), p2.getPrev())
            + Math.min(p1.getCurr(), p2.getCurr())
            + Math.min(p1.getSufix(), p2.getSufix());
    return comm;
  }

  public static int getCommonTokensNum(int[] tokens1, int[] tokens2) {
    int result = 0;
    int c1 = tokens1.length;
    int c2 = tokens2.length;
    int i = 0;
    int j = 0;
    while (i < c1 && j < c2) {
      if (tokens1[i] < tokens2[j]) {
        i++;
      } else if (tokens1[i] > tokens2[j]) {
        j++;
      } else {
        result++;
        i++;
        j++;
      }
    }
    //System.out.println("Common:"+result);
    return result;
  }

}
