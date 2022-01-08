/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package de.huberlin.textualsimilarityhadoop;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

/**
 *
 * @author fabi
 */
public class DetectInputDataType {
  public enum DataType { INTEGERTOKENIZED, STRINGANDINTEGERTOKENIZED, RAW, UNRECOGNIZED };
  public boolean hasSecondInput = false;
  
  
  public DetectInputDataType() {
  }
  
  public DataType detect(Path pt, FileSystem filesystem) {
    try {
      if (filesystem.exists(new Path(pt.toString() + "2"))) {
        hasSecondInput = true;
      }
      
      // if the path is a directory, we grab the first file in the directory
      while (filesystem.isDirectory(pt)) {
        RemoteIterator<LocatedFileStatus> it = filesystem.listFiles(pt, true);
        while (it.hasNext()) {
          LocatedFileStatus s = it.next();
          if (s.isFile()) { // we take the first file we find
            pt = s.getPath();
            if (!pt.getName().equals("_SUCCESS")) { // we make sure this file is not the empty _SUCCESS file
              break;
            }
          }
        }
      }
      BufferedReader br = new BufferedReader(new InputStreamReader(filesystem.open(pt)));
      String line = br.readLine();
      while (line != null) {
        String[] tmp = line.split("\\s+");
        
        try {
          Integer.parseInt(tmp[0]);
          if (tmp.length <= 2) {
            // here we are pretty sure that the input is already tokenized to integers
            System.out.println("Recognized integer-tokenized input");
            return DataType.INTEGERTOKENIZED;
          } else {
            // here we guess that the input is tokenized to integers with original string behind
            System.out.println("Recognized integer-tokenized input including original string data");
            return DataType.STRINGANDINTEGERTOKENIZED;
          }
        } catch (Exception e) {
          // here we might think that this is raw text input -> End-To-End + Tokenization
          System.out.println("Recognized raw text input");
          return DataType.RAW;
        }
      }
      br.close();
    } catch(Exception e) {
      System.out.println("Could not recognize input data type. Aborting. " + e);
      System.exit(-1);
    }
    System.out.println("Unrecognized data!");
    return DataType.UNRECOGNIZED;
  }
  
}
