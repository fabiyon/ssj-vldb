package de.huberlin.textualsimilarityhadoop;

import edu.uci.ics.fuzzyjoin.similarity.SimilarityFiltersJaccard;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

public class PPJoinPlus {
  private final TreeMap<Integer, ArrayList<long[]>> invertedList;
  private final SimilarityFiltersJaccard similarityFilters;
  private final HashMap<Long, int[]> recordsNew; // rid, tokens

  public PPJoinPlus(float similarityThreshold) {
    invertedList = new TreeMap();
    similarityFilters = new SimilarityFiltersJaccard(similarityThreshold);
    recordsNew = new HashMap();
  }
  
  public void runSelfJoin(Collection<int[]> records, ArrayList<Integer> rids, String outFileName) throws FileNotFoundException, UnsupportedEncodingException {
    if (outFileName != null) {
      PrintWriter writer = new PrintWriter(outFileName, "UTF-8");
      int count = 0;
      for (int[] record : records) {
        ArrayList<Long> results = new ArrayList();
        int currentRecordId = rids.get(count++);
        selfJoinAndAddRecord(record, currentRecordId, results);
        for (Long result : results) {
          writer.println(currentRecordId + " " + result);
//          writer.format("%d %d", currentRecordId, result);
//          writer.println();
        }
      }
      writer.close();
    } else { // we are not interested in the results:
      int count = 0;
      for (int[] record : records) {
        selfJoinAndAddRecord(record, rids.get(count++)); // <<<<< I added this count in order to be able to use selfJoinAndAddRecord from outside classes. Didn't test it yet.
      }
    }
  }

  public void selfJoinAndAddRecord(final int[] tokens, int index) {
    selfJoinAndAddRecord(tokens, index, null);
  }
  
  public void addTokenToIndex(int token, long recordId, int tokenPosition, int recordLength) {
    ArrayList<long[]> tmp = invertedList.get(token);
    if (tmp == null) {
      tmp = new ArrayList();
    }
    tmp.add(new long[]{recordId, tokenPosition, recordLength});
    invertedList.put(token, tmp);
  }
  
  public void addRecord(final int[] tokens, long index) {
    final int length = tokens.length;
    final int indexPrefixLength = similarityFilters.getIndexPrefixLength(length);
    for (int indexToken = 0; indexToken < indexPrefixLength; indexToken++) {
      final int token = tokens[indexToken];
      addTokenToIndex(token, index, indexToken, length);
    }
    recordsNew.put(index, tokens); // we need all previous records for the verification
  }
  
  // for self-join:
  public void selfJoinAndAddRecord(final int[] tokens, long index, ArrayList<Long> results) {
    joinRecord(tokens, index, results, true);
  }
  
  // for non-self-join:
  public void probeRecord(final int[] tokens, long index, ArrayList<Long> results) {
    joinRecord(tokens, index, results, false);
  }
  
  public void joinRecord(final int[] tokens, long index, ArrayList<Long> results, boolean doAddRecord) {
//    if (index == 382 || index == 383) {
//      System.out.println();
//    }
    final int length = tokens.length;
    final int prefixLength = similarityFilters.getPrefixLength(length);
    final int indexPrefixLength = prefixLength; //similarityFilters.getIndexPrefixLength(length); // using the index prefix causes loss of record pairs in MapReduce, because we cannot make sure the order of incoming records
    final int lengthLowerBound = similarityFilters.getLengthLowerBound(length);
    //
    // self join
    //
    final HashMap<Long, Integer> counts = new HashMap();
    for (int indexToken = 0; indexToken < prefixLength; indexToken++) {
      final int token = tokens[indexToken];
      //
      // probe index
      //
      ArrayList<long[]> currentInvertedList = invertedList.get(token);
      if (currentInvertedList != null) {
        Iterator<long[]> currentInvertedListIterator = currentInvertedList.iterator();
        while (currentInvertedListIterator.hasNext()) {
          long[] element = currentInvertedListIterator.next();
          final long indexProbe = element[0];
          final int indexTokenProbe = (int)element[1];
          final int lengthProbe = (int)element[2];
          
          // length filter:
          // an optimization would be to also delete these entries if we are sure the index
          // is not needed afterwards:
          if (lengthProbe < lengthLowerBound) {
            continue;
          }
          // Removed, because VernicaHadoop misses results if we apply this optimization:
//          if (indexProbe >= index) { // in case the index is pre-built, this prevents from generating duplicates
//            // It does not make a difference if it is >= or >...
//            break;
//          }

          Integer count = counts.get(indexProbe);
          if (count == null) {
            count = 0;
          }

          if (count != -1) {
            count++;
            // position filter
            if (!similarityFilters.passPositionFilter(count,
                    indexToken, length, indexTokenProbe,
                    lengthProbe)) {
              count = -1;
            }
            // suffix filter <<<<<<<<<<<<< Break with generated input data 100000_10000_.5_200.out as raw dataset:
            /**
             * Error: java.lang.ArrayIndexOutOfBoundsException: 6
	at edu.uci.ics.fuzzyjoin.similarity.SimilarityFiltersJaccard.getSuffixFilter(SimilarityFiltersJaccard.java:159)
	at edu.uci.ics.fuzzyjoin.similarity.SimilarityFiltersJaccard.passSuffixFilter(SimilarityFiltersJaccard.java:327)
	at edu.uci.ics.fuzzyjoin.similarity.SimilarityFiltersJaccard.passSuffixFilter(SimilarityFiltersJaccard.java:346)
	at de.huberlin.textualsimilarityhadoop.PPJoinPlus.joinRecord(PPJoinPlus.java:133)
             */
//            if (count == 1
//                    && !similarityFilters.passSuffixFilter(tokens,
//                            indexToken, recordsNew.get(indexProbe),
//                            indexTokenProbe)) {
//              count = -1;
//            }
            counts.put(indexProbe, count);
          }
          
        }
      }
      //
      // add to index
      //
      if (doAddRecord && indexToken < indexPrefixLength) {
        addTokenToIndex(token, index, indexToken, length);
      }
    }
    //
    // add record
    //
    if (doAddRecord) {
      recordsNew.put(index, tokens);
    }
    //
    // verify candidates
    //
    for (Map.Entry<Long, Integer> cand : counts.entrySet()) {
      int count = cand.getValue();
      long indexProbe = cand.getKey();
      if (count > 0) {
        int tokensProbe[] = recordsNew.get(indexProbe);
        float similarity = similarityFilters.passSimilarityFilter(
                tokens, prefixLength, tokensProbe, 
                similarityFilters.getIndexPrefixLength(tokensProbe.length),
                count);
        if (similarity > 0) {
          if (results != null) {
            results.add(indexProbe); // this record is similar to the record with rid=index
          } else {
            // this is not a useful case: this function has to be called with an initialized object
          }
        }
      }
    }
  }

}
