/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.streams.kstream;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Random;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Percentile;
import org.apache.kafka.common.metrics.stats.Percentiles;
import org.apache.kafka.common.metrics.stats.Percentiles.BucketSizing;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.junit.Test;

import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.time.Duration.ofMillis;
import static org.apache.kafka.streams.EqualityCheck.verifyEquality;
import static org.apache.kafka.streams.EqualityCheck.verifyInEquality;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.fail;

@SuppressWarnings("deprecation")
public class TimeWindowsTest {

    private static final long ANY_SIZE = 123L;

    @Test
    public void shouldSetWindowSize() {
        assertEquals(ANY_SIZE, TimeWindows.of(ofMillis(ANY_SIZE)).sizeMs);
    }

    @Test
    public void shouldSetWindowAdvance() {
        final long anyAdvance = 4;
        assertEquals(anyAdvance, TimeWindows.of(ofMillis(ANY_SIZE)).advanceBy(ofMillis(anyAdvance)).advanceMs);
    }

    @SuppressWarnings("deprecation") // specifically testing deprecated APIs
    @Test
    public void shouldSetWindowRetentionTime() {
        assertEquals(ANY_SIZE, TimeWindows.of(ofMillis(ANY_SIZE)).until(ANY_SIZE).maintainMs());
    }

    @SuppressWarnings("deprecation") // specifically testing deprecated APIs
    @Test
    public void shouldUseWindowSizeAsRentitionTimeIfWindowSizeIsLargerThanDefaultRetentionTime() {
        final long windowSize = 2 * TimeWindows.of(ofMillis(1)).maintainMs();
        assertEquals(windowSize, TimeWindows.of(ofMillis(windowSize)).maintainMs());
    }

    @Test(expected = IllegalArgumentException.class)
    public void windowSizeMustNotBeZero() {
        TimeWindows.of(ofMillis(0));
    }

    @Test(expected = IllegalArgumentException.class)
    public void windowSizeMustNotBeNegative() {
        TimeWindows.of(ofMillis(-1));
    }

    @Test
    public void advanceIntervalMustNotBeZero() {
        final TimeWindows windowSpec = TimeWindows.of(ofMillis(ANY_SIZE));
        try {
            windowSpec.advanceBy(ofMillis(0));
            fail("should not accept zero advance parameter");
        } catch (final IllegalArgumentException e) {
            // expected
        }
    }

    @Test
    public void advanceIntervalMustNotBeNegative() {
        final TimeWindows windowSpec = TimeWindows.of(ofMillis(ANY_SIZE));
        try {
            windowSpec.advanceBy(ofMillis(-1));
            fail("should not accept negative advance parameter");
        } catch (final IllegalArgumentException e) {
            // expected
        }
    }

    @Deprecated
    @Test
    public void advanceIntervalMustNotBeLargerThanWindowSize() {
        final TimeWindows windowSpec = TimeWindows.of(ofMillis(ANY_SIZE));
        try {
            windowSpec.advanceBy(ofMillis(ANY_SIZE + 1));
            fail("should not accept advance greater than window size");
        } catch (final IllegalArgumentException e) {
            // expected
        }
    }

    @Deprecated
    @Test
    public void retentionTimeMustNoBeSmallerThanWindowSize() {
        final TimeWindows windowSpec = TimeWindows.of(ofMillis(ANY_SIZE));
        try {
            windowSpec.until(ANY_SIZE - 1);
            fail("should not accept retention time smaller than window size");
        } catch (final IllegalArgumentException e) {
            // expected
        }
    }

    @Test
    public void gracePeriodShouldEnforceBoundaries() {
        TimeWindows.of(ofMillis(3L)).grace(ofMillis(0L));

        try {
            TimeWindows.of(ofMillis(3L)).grace(ofMillis(-1L));
            fail("should not accept negatives");
        } catch (final IllegalArgumentException e) {
            //expected
        }
    }

    @Test
    public void shouldComputeWindowsForHoppingWindows() {
        final TimeWindows windows = TimeWindows.of(ofMillis(12L)).advanceBy(ofMillis(5L));
        final Map<Long, TimeWindow> matched = windows.windowsFor(21L);
        assertEquals(12L / 5L + 1, matched.size());
        assertEquals(new TimeWindow(10L, 22L), matched.get(10L));
        assertEquals(new TimeWindow(15L, 27L), matched.get(15L));
        assertEquals(new TimeWindow(20L, 32L), matched.get(20L));
    }

    @Test
    public void shouldComputeWindowsForBarelyOverlappingHoppingWindows() {
        final TimeWindows windows = TimeWindows.of(ofMillis(6L)).advanceBy(ofMillis(5L));
        final Map<Long, TimeWindow> matched = windows.windowsFor(7L);
        assertEquals(1, matched.size());
        assertEquals(new TimeWindow(5L, 11L), matched.get(5L));
    }

    @Test
    public void shouldComputeWindowsForTumblingWindows() {
        final TimeWindows windows = TimeWindows.of(ofMillis(12L));
        final Map<Long, TimeWindow> matched = windows.windowsFor(21L);
        assertEquals(1, matched.size());
        assertEquals(new TimeWindow(12L, 24L), matched.get(12L));
    }


    @Test
    public void equalsAndHashcodeShouldBeValidForPositiveCases() {
        verifyEquality(TimeWindows.of(ofMillis(3)), TimeWindows.of(ofMillis(3)));

        verifyEquality(TimeWindows.of(ofMillis(3)).advanceBy(ofMillis(1)), TimeWindows.of(ofMillis(3)).advanceBy(ofMillis(1)));

        verifyEquality(TimeWindows.of(ofMillis(3)).grace(ofMillis(1)), TimeWindows.of(ofMillis(3)).grace(ofMillis(1)));

        verifyEquality(TimeWindows.of(ofMillis(3)).grace(ofMillis(4)), TimeWindows.of(ofMillis(3)).grace(ofMillis(4)));

        verifyEquality(
            TimeWindows.of(ofMillis(3)).advanceBy(ofMillis(1)).grace(ofMillis(1)).grace(ofMillis(4)),
            TimeWindows.of(ofMillis(3)).advanceBy(ofMillis(1)).grace(ofMillis(1)).grace(ofMillis(4))
        );
    }

    @Test
    public void equalsAndHashcodeShouldBeValidForNegativeCases() {
        verifyInEquality(TimeWindows.of(ofMillis(9)), TimeWindows.of(ofMillis(3)));

        verifyInEquality(TimeWindows.of(ofMillis(3)).advanceBy(ofMillis(2)), TimeWindows.of(ofMillis(3)).advanceBy(ofMillis(1)));

        verifyInEquality(TimeWindows.of(ofMillis(3)).grace(ofMillis(2)), TimeWindows.of(ofMillis(3)).grace(ofMillis(1)));

        verifyInEquality(TimeWindows.of(ofMillis(3)).grace(ofMillis(9)), TimeWindows.of(ofMillis(3)).grace(ofMillis(4)));


        verifyInEquality(
            TimeWindows.of(ofMillis(4)).advanceBy(ofMillis(2)).grace(ofMillis(2)),
            TimeWindows.of(ofMillis(3)).advanceBy(ofMillis(2)).grace(ofMillis(2))
        );

        verifyInEquality(
            TimeWindows.of(ofMillis(3)).advanceBy(ofMillis(1)).grace(ofMillis(2)),
            TimeWindows.of(ofMillis(3)).advanceBy(ofMillis(2)).grace(ofMillis(2))
        );

        assertNotEquals(
            TimeWindows.of(ofMillis(3)).advanceBy(ofMillis(2)).grace(ofMillis(1)),
            TimeWindows.of(ofMillis(3)).advanceBy(ofMillis(2)).grace(ofMillis(2))
        );
    }

    final Logger log = LoggerFactory.getLogger(TimeWindowsTest.class);

    @Test
    public void testSlidingWindowClaims() {

        long windowSize = 3;
        Window.windowSize = windowSize;
        long timeRange = (windowSize * 2);

        int i = 0;
        for (final List<ValueAndTimestamp<String>> allRecords : getAllInputRecordCombinations(timeRange)) {
            final SortedMap<Long, Window> windows = new TreeMap<>();
            int j = 0;

            log.info("On {}th run, number of input records is {}", i, allRecords.size());

            for (final List<ValueAndTimestamp<String>> records : generatePerm(allRecords)) {

                log.info("STARTING {}-{}th run", i, j);

                for (final ValueAndTimestamp<String> record : records) {
                    final SortedMap<Long, Window> windowsFor = windows.subMap(
                        record.timestamp() - (3 * windowSize),
                        record.timestamp() + (3 * windowSize)
                    );

                    final long typeAStart = record.timestamp() + 1;
                    final long typeAEnd = typeAStart + windowSize;
                    if (windowsFor.containsKey(typeAStart)) {
                        windowsFor.get(typeAStart).aggregate(record.value(), record.timestamp());
                    } else {
                        boolean recordsInRangeExist = false;
                        String preAggregate = null;
                        long min = Long.MAX_VALUE;
                        long max = Long.MIN_VALUE;
                        for (final Window window : windowsFor.values()) {
                            if (window.isEmpty) {
                                continue;
                            }
                            if (window.start < typeAStart && window.maxRecord >= typeAStart) {
                                recordsInRangeExist = true;
                                if (window.minRecord >= typeAStart && window.minRecord < min) {
                                    preAggregate = window.agg;
                                    min = window.minRecord;
                                }
                            } else if (window.start > typeAStart && window.minRecord <= typeAEnd) {
                                recordsInRangeExist = true;
                                if (window.maxRecord <= typeAEnd && window.maxRecord > max) {
                                    preAggregate = window.agg;
                                    max = window.maxRecord;
                                }
                            }
                        }
                        if (recordsInRangeExist && preAggregate == null) {
                            System.err.println("Failed on " + i + "th run with records = " + records);
                            System.err.println("Existing windows = " + windows);
                            throw new AssertionError("Could not find the preaggregate for record " + record + " on run #" + i);
                        } else {
                            windows.put(typeAStart, Window.typeAWindow(typeAStart, min, max, preAggregate));
                        }
                    }

                    final long typeBStart = record.timestamp() - windowSize;
                    final long typeBEnd = record.timestamp();
                    if (windowsFor.containsKey(typeBStart)) {
                        windowsFor.get(typeBStart).aggregate(record.value(), record.timestamp());
                    } else {
                        boolean recordsInRangeExist = false;
                        String preAggregate = null;
                        long min = record.timestamp();
                        for (final Window window : windowsFor.values()) {
                            if (window.isEmpty) {
                                continue;
                            }
                            if (window.start < typeBStart && window.maxRecord >= typeBStart) {
                                recordsInRangeExist = true;
                                if (window.minRecord >= typeBStart) {
                                    min = window.minRecord;
                                    preAggregate = window.agg;
                                }
                            } else if (window.start > typeBStart && window.minRecord <= typeBEnd) {
                                recordsInRangeExist = true;
                                if (window.maxRecord <= typeBEnd) {
                                    min = window.minRecord;
                                    preAggregate = window.agg;
                                }
                            }
                        }
                        if (recordsInRangeExist && preAggregate == null) {
                            System.err.println("Failed on " + i + "th run with records = " + records);
                            System.err.println("Existing windows = " + windows);
                            throw new AssertionError("Could not find the preaggregate for record " + record + "on run #" + i);
                        } else {
                            windows.put(typeBStart, Window.typeBWindow(typeBEnd, min, preAggregate, record.value()));
                        }
                    }

                    // Update existing windows in range
                    for (final Window window : windowsFor.values()) {
                        if (window.start <= record.timestamp() && window.end >= record.timestamp()) {
                            window.aggregate(record.value(), record.timestamp());
                        }
                    }
                }
                ++j;
            }

            ++i;
        }
    }

    public List<List<ValueAndTimestamp<String>>> generatePerm(List<ValueAndTimestamp<String>> original) {
        if (original.isEmpty()) {
            List<List<ValueAndTimestamp<String>>> result = new ArrayList<>();
            result.add(new ArrayList<>());
            return result;
        }
        ValueAndTimestamp<String> firstElement = original.remove(0);
        List<List<ValueAndTimestamp<String>>> returnValue = new ArrayList<>();
        List<List<ValueAndTimestamp<String>>> permutations = generatePerm(original);
        for (List<ValueAndTimestamp<String>> smallerPermutated : permutations) {
            for (int index=0; index <= smallerPermutated.size(); index++) {
                List<ValueAndTimestamp<String>> temp = new ArrayList<>(smallerPermutated);
                temp.add(index, firstElement);
                returnValue.add(temp);
            }
        }
        return returnValue;
    }

    private List<LinkedList<ValueAndTimestamp<String>>> getAllInputRecordCombinations(final long timeRange) {
        return getAllInputRecordCombinations(timeRange, new ArrayList<>());
    }

    private List<LinkedList<ValueAndTimestamp<String>>> getAllInputRecordCombinations(final long time,
                                                                                      final List<LinkedList<ValueAndTimestamp<String>>> cur) {
        if (time < 0) {
            return  cur;
        }

        final List<LinkedList<ValueAndTimestamp<String>>> newLists = new ArrayList<>();
        if (cur.isEmpty()) {
            final LinkedList<ValueAndTimestamp<String>> list = new LinkedList<>();
            list.add(ValueAndTimestamp.make(String.valueOf(time), time));

            newLists.add(list);
            newLists.add(new LinkedList<>());
        } else {
            for (final LinkedList<ValueAndTimestamp<String>> list : cur) {
                final LinkedList<ValueAndTimestamp<String>> copy = new LinkedList<>(list);
                copy.addFirst(ValueAndTimestamp.make(String.valueOf(time), time));
                newLists.add(copy);
            }
        }
        cur.addAll(newLists);
        return getAllInputRecordCombinations(time - 1, cur);
    }

    static class Window {
        static long windowSize;

        final long start;
        final long end;
        long minRecord;
        long maxRecord;
        String agg;
        boolean isEmpty;

        Window(final long start, final long end, final String initialValue, long minRecord, long maxRecord, final boolean isEmpty) {
            this.start = start;
            this.end = end;
            agg = initialValue;
            this.maxRecord = maxRecord;
            this.minRecord = minRecord;
            this.isEmpty = isEmpty;
        }

        Window aggregate(final String value, final long newTimestamp) {
            if (!isEmpty) {
                agg += "-";
            }
            agg += value;
            isEmpty = false;
            if (newTimestamp > maxRecord) {
                maxRecord = newTimestamp;
            } else if (newTimestamp < minRecord) {
                minRecord = newTimestamp;
            }
            return  this;
        }

        static Window typeAWindow(final long start, final long min, final long max, final String preAgg) {
            if (preAgg == null) {
                return new Window(start, start + windowSize, "", min, max, true);
            } else {
                return new Window(start, start + windowSize, preAgg, min, max, false);
            }
        }

        static Window typeBWindow(final long end, final long minRec, final String preAgg, final String newValue) {
            final String init = preAgg == null ? newValue : preAgg + "-" + newValue;

            return new Window(end - windowSize, end, init, minRec, end, false);
        }


    }

}
