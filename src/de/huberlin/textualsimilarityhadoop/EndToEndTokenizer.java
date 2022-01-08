package de.huberlin.textualsimilarityhadoop;

import de.huberlin.tokenizer.Tokenizer;
import de.huberlin.tokenizer.TokenizerFactory;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class EndToEndTokenizer {

  /**
   * Assign RIDs
   */
  public static class TokenizeAssignRidMapper extends Mapper<Object, Text, Text, NullWritable> {
    private final Text record = new Text();
    private long rid = 1;
    int increaseFactor;
    
    @Override
    protected void setup(Context context) {
      increaseFactor = Integer.parseInt(context.getConfiguration().get("increaseFactor", "1"));
    }
    
    
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      record.set("" + (rid) + " " + value.toString());
      rid = rid + increaseFactor; // we leave some space here for the newly generated records
      context.write(record, NullWritable.get());
    }
  }
  
  /**
   * Extract all tokens
   */
  public static class TokenizeMapper extends Mapper<Object, Text, Text, NullWritable> {
    private final Text outKey = new Text();
    private final NullWritable outValue = NullWritable.get();
    private Tokenizer tokenizer;
    
    @Override
    protected void setup(Context context) {
      String tokenizerChoice = context.getConfiguration().get("tokenizerChoice", "BASIC");
      tokenizer = TokenizerFactory.getTokenizer(tokenizerChoice);
    }

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String[] valueArr = tokenizer.split(value.toString());
      if (valueArr.length == 0) {
        return;
      }
      for (String token : valueArr) {
        outKey.set(token);
        context.write(outKey, outValue);
      }      
    }
  }
  
  /**
   * We need each token exactly once, thus we use a reducer:
   */
  public static class TokenizeReducer extends Reducer<Text, NullWritable, Text, NullWritable> {
    private final NullWritable outVal = NullWritable.get();

    @Override
    public void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
      context.write(key, outVal);
    }
  }
  
  
  /**
   * Replace string tokens with integer tokens:
   */
  public static class TokenizeAssignMapper extends Mapper<Object, Text, Text, NullWritable> {
    private final Text outKey = new Text();
    private final NullWritable outValue = NullWritable.get();
    private final HashMap<String, Integer> tokensNew = new HashMap();
    private Tokenizer tokenizer;
    int increaseFactor;
    int maxTokenId;
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      String tokenizerChoice = context.getConfiguration().get("tokenizerChoice", "BASIC");
      tokenizer = TokenizerFactory.getTokenizer(tokenizerChoice);
      tokenizer.setWithRid(true);
      // read token list for tokenization:
      String tokenString = context.getConfiguration().get("tokenString", "");
      if (!tokenString.isEmpty()) { // this path is only set if we have input that needs to be tokenized
        try {
          Path pt = new Path(tokenString);
          FileSystem fs = FileSystem.get(new Configuration());
          RemoteIterator<LocatedFileStatus> it = fs.listFiles(pt, true);
          int currentTokenId = 0;
          while (it.hasNext()) {
            LocatedFileStatus lfs = it.next();
            if (lfs.isFile()) {
              BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(lfs.getPath())));
              String line = br.readLine();
              while (line != null) {
//                tokens.add(line);
                tokensNew.put(line, currentTokenId++);
                line = br.readLine();
              }
            }
          }
          maxTokenId = currentTokenId - 1;
        } catch(Exception e) {
        }
      }
      increaseFactor = Integer.parseInt(context.getConfiguration().get("increaseFactor", "1"));
    }

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String inputline = value.toString();
      int rid = -1;
      if (context.getConfiguration().get("tokenizerChoice", "BASIC").equals("FS")) {
        int index = inputline.indexOf(" ");
        rid = Integer.parseInt(inputline.substring(0, index));
        inputline = inputline.substring(index + 1).trim();
      }
      
      String[] valueArr = tokenizer.split(inputline);
      
      if (valueArr.length == 0) {
        return;
      }
      ArrayList<Integer> tmpList = new ArrayList(); // we use a temporary list to sort the tokens
      for (String token : valueArr) {
        if (rid == -1) {
          rid = Integer.parseInt(token);
        } else {
          Integer tokenInt = tokensNew.get(token);
          if (tokenInt != null && !tmpList.contains(tokenInt)) { // manchmal fehlen bei der Vernica-Tokenisierung einige Tokens in der Liste, was beim Sortieren eine NullPointerException auslöst. Zudem können bei der Basic-Tokenisierung hier doppelte Tokens auftauchen
            tmpList.add(tokenInt);
          }
        }
      }
      Collections.sort(tmpList);
      
      
      String tokenString = "";
      for (Integer token : tmpList) {
        if (!tokenString.isEmpty()) {
          tokenString += ",";
        }
        tokenString += token;
      }
      outKey.set(rid + " " + tokenString);// + " " + value.toString());
      context.write(outKey, outValue);
      
      for (int i = 1; i < increaseFactor; i++) {
        rid++;
        String increasedTokenString = "";
        for (Integer token : tmpList) {
          if (!increasedTokenString.isEmpty()) {
            increasedTokenString += ",";
          }
          int increasedToken = (token + i) % maxTokenId; // <<<<<<<<<<< falls der Modulo eingesetzt wird müsste man eigentlich nochmal am Anfang anfügen...
          increasedTokenString += increasedToken;
        }
        outKey.set(rid + " " + increasedTokenString);// + " " + value.toString());
        context.write(outKey, outValue);
      }
    }
  }

}
