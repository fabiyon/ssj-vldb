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

import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.WritableComparator;

public class IntPairComparatorFirst implements RawComparator<IntPairWritable> {

    public static int compareInt(int i1, int i2) {
        return i1 < i2 ? -1 : i1 == i2 ? 0 : 1;
    }

    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
        return WritableComparator.compareBytes(b1, s1, 4, b2, s2, 4);
    }

    @Override
    public int compare(IntPairWritable o1, IntPairWritable o2) {
        return compareInt(o1.getFirst(), o2.getFirst());
    }
}
