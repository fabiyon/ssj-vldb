/**
 * Copyright 2010-2011 The Regents of the University of California
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on
 * an "AS IS"; BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under
 * the License.
 * 
 * Author: Rares Vernica <rares (at) ics.uci.edu>
 */

package de.huberlin.vernicajoin;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AsymFuzzyJoinMemory {
    private final InvertedListsLengthList invertedLists;
    private final SimilarityFiltersJaccard similarityFilters;

    private final ArrayList<int[]> records;

    public AsymFuzzyJoinMemory(float similarityThreshold) {
        invertedLists = new InvertedListsLengthList();
        similarityFilters = new SimilarityFiltersJaccard(similarityThreshold);
        records = new ArrayList<int[]>();
    }

    public void add(final int[] tokens) {
        final int index = records.size();
        final int length = tokens.length;
        final int indexPrefixLength = similarityFilters.getPrefixLength(length);

        for (int indexToken = 0; indexToken < indexPrefixLength; indexToken++) {
            invertedLists.index(tokens[indexToken], new int[] { index,
                    indexToken, length });
        }
        records.add(tokens);
    }

    public ArrayList<ResultJoin> join(final int[] tokens, final int length) {
        final int prefixLength = similarityFilters.getPrefixLength(length);
        final int lengthLowerBound = similarityFilters
                .getLengthLowerBound(length);
        //
        // self join
        //
        final HashMap<Integer, Integer> counts = new HashMap<Integer, Integer>();
        for (int indexToken = 0; indexToken < Math.min(prefixLength,
                tokens.length); indexToken++) {
            final int token = tokens[indexToken];
            //
            // probe index
            //
            InvertedListLengthList invertedList = invertedLists.get(token);
            if (invertedList != null) {
                // length filter
//                invertedList.setMinLength(lengthLowerBound); // we can't prune the index, because we use it several times for different partitions
                for (int[] element : invertedList) {
                    final int indexProbe = element[0];
                    final int indexTokenProbe = element[1];
                    final int lengthProbe = element[2];
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
                        // suffix filter
                        if (count == 1
                                && !similarityFilters.passSuffixFilter(tokens,
                                        indexToken, records.get(indexProbe),
                                        indexTokenProbe)) {
                            count = -1;
                        }
                        counts.put(indexProbe, count);
                    }
                }
            }
        }
        //
        // verify candidates
        //
        ArrayList<ResultJoin> results = new ArrayList<ResultJoin>();
        for (Map.Entry<Integer, Integer> cand : counts.entrySet()) {
            int count = cand.getValue();
            int indexProbe = cand.getKey();
            if (count > 0) {
                int tokensProbe[] = records.get(indexProbe);
                float similarity = similarityFilters.passSimilarityFilter(
                        tokens, prefixLength, tokensProbe,
                        similarityFilters.getPrefixLength(tokensProbe.length),
                        count);
                if (similarity > 0) {
                    results.add(new ResultJoin(indexProbe, similarity));
                }
            }
        }
        return results;
    }

    public void prune(int length) {
        final int lengthLowerBound = similarityFilters
                .getLengthLowerBound(length + 1);
        invertedLists.prune(lengthLowerBound);
    }

    public List<ResultSelfJoin> runs(Collection<int[]> records, int noRuns,
            int warmupFactor) {
        if (records.size() < 2) {
            return new ArrayList<ResultSelfJoin>();
        }

        int noRunsTotal = noRuns * warmupFactor;
        float runtime = 0, runtimeAverage = 0;
        ArrayList<ResultSelfJoin> results = new ArrayList<ResultSelfJoin>();

        System.err.println("# Records: " + records.size());
        System.err.print("=== BEGIN JOIN (TIMER STARTED) === ");
        for (int i = 1; i <= noRunsTotal; i++) {
            System.err.print(".");
            System.err.flush();

            results.clear();
            Runtime.getRuntime().gc();

            Date startTime = new Date();
            for (int[] record : records) {
                results.addAll(selfJoinAndAddRecord(record));
            }
            Date endTime = new Date();
            runtime = (endTime.getTime() - startTime.getTime())
                    / (float) 1000.0;

            if (i >= noRunsTotal - noRuns) {
                runtimeAverage += runtime;
            }
        }
        System.err.println();
        System.err.println("# Results: " + results.size());
        System.err.println("=== END JOIN (TIMER STOPPED) ===");
        System.err.println("Total Running Time:  " + runtimeAverage / noRuns
                + " (" + runtime + ")");
        System.err.println();
        return results;
    }

    public ArrayList<ResultSelfJoin> selfJoinAndAddRecord(final int[] tokens) {
        final int index = records.size();
        final int length = tokens.length;
        final int prefixLength = similarityFilters.getPrefixLength(length);
        final int indexPrefixLength = similarityFilters
                .getIndexPrefixLength(length);
        final int lengthLowerBound = similarityFilters
                .getLengthLowerBound(length);
        //
        // self join
        //
        final HashMap<Integer, Integer> counts = new HashMap<Integer, Integer>();
        for (int indexToken = 0; indexToken < prefixLength; indexToken++) {
            final int token = tokens[indexToken];
            //
            // probe index
            //
            InvertedListLengthList invertedList = invertedLists.get(token);
            if (invertedList != null) {
                // length filter
                invertedList.setMinLength(lengthLowerBound);
                for (int[] element : invertedList) {
                    final int indexProbe = element[0];
                    final int indexTokenProbe = element[1];
                    final int lengthProbe = element[2];
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
                        // suffix filter
                        if (count == 1
                                && !similarityFilters.passSuffixFilter(tokens,
                                        indexToken, records.get(indexProbe),
                                        indexTokenProbe)) {
                            count = -1;
                        }
                        counts.put(indexProbe, count);
                    }
                }
            }
            //
            // add to index
            //
            if (indexToken < indexPrefixLength) {
                invertedLists.index(token, new int[] { index, indexToken,
                        length });
            }
        }
        //
        // add record
        //
        records.add(tokens);
        //
        // verify candidates
        //
        ArrayList<ResultSelfJoin> results = new ArrayList<ResultSelfJoin>();
        for (Map.Entry<Integer, Integer> cand : counts.entrySet()) {
            int count = cand.getValue();
            int indexProbe = cand.getKey();
            if (count > 0) {
                int tokensProbe[] = records.get(indexProbe);
                float similarity = similarityFilters.passSimilarityFilter(
                        tokens, prefixLength, tokensProbe, similarityFilters
                                .getIndexPrefixLength(tokensProbe.length),
                        count);
                if (similarity > 0) {
                    results.add(new ResultSelfJoin(index, indexProbe,
                            similarity));
                }
            }
        }
        return results;
    }
}
