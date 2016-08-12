/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package timely.store.iterators;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.IteratorSetting.Column;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iterators.OptionDescriber;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.conf.ColumnSet;
import org.apache.accumulo.core.util.ComparablePair;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import timely.api.model.Metric;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Lists;

/**
 * This is a copy of org.apache.accumulo.core.iterators.Combiner with changes.
 * The Combiner class will reduce all values with matching row, colf, colq, and
 * colviz into one value. This iterator will reduce all values with matching
 * row, colf, colq, and colviz into multiple K,V pairs based on the configured
 * time window. The returned Key will be the first key in the time window.
 * Additionally, the reduce method signature has changed to allow the passing of
 * the Key and Value that are being reduced.
 * 
 */
public abstract class TimeWindowCombiner implements SortedKeyValueIterator<Key, Value>, OptionDescriber {

    /**
     * A Java Iterator that returns a Key and Value when the given Key is within
     * the time window.
     */
    public static class TimeWindowValueIterator implements Iterator<KeyValuePair> {

        private final Key startKey;
        private final LookaheadIterator source;
        private final String startMetric;
        private final byte[] startColf;
        private final byte[] startColq;
        private boolean hasNext;
        private long window;

        public TimeWindowValueIterator(LookaheadIterator source, long window) throws IOException {
            this.source = source;
            startKey = new Key(source.getTopKey());
            ComparablePair<String, Long> row = Metric.decodeRowKey(startKey.getRow().getBytes());
            this.startMetric = row.getFirst();
            this.startColf = startKey.getColumnFamily().getBytes();
            this.startColq = startKey.getColumnQualifier().getBytes();
            this.window = window;
            hasNext = isInWindow(startKey);
        }

        /**
         * Test whether the given key is in the time window compared to the
         * start key
         * 
         * @param test
         *            key
         * @return
         */
        private boolean isInWindow(Key test) {
            if (!test.isDeleted() && (test.getTimestamp() - this.startKey.getTimestamp()) < this.window) {
                ComparablePair<String, Long> row = Metric.decodeRowKey(test.getRow().getBytes());
                return (startMetric.equals(row.getFirst())
                        && Arrays.equals(startColf, test.getColumnFamily().getBytes()) && Arrays.equals(startColq, test
                        .getColumnQualifier().getBytes()));
            }
            return false;
        }

        public boolean hasNext() {
            return hasNext;
        }

        public KeyValuePair next() {
            if (!hasNext)
                throw new NoSuchElementException();
            // Populate the response
            KeyValuePair result = new KeyValuePair();
            result.setKey(source.getTopKey());
            result.setValue(source.getTopValue());
            // Lookahead at the next key and test it
            try {
                KeyValuePair lookahead = source.peek();
                if (null != lookahead) {
                    if (isInWindow(lookahead.getKey())) {
                        hasNext = true;
                        source.next();
                    } else {
                        hasNext = false;
                    }
                } else {
                    hasNext = false;
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return result;
        }

        /**
         * This method is unsupported in this iterator.
         *
         * @throws UnsupportedOperationException
         *             when called
         */
        public void remove() {
            throw new UnsupportedOperationException();
        }

    }

    static final Logger sawDeleteLog = LoggerFactory.getLogger(TimeWindowCombiner.class.getName() + ".SawDelete");

    protected static final String COLUMNS_OPTION = "columns";
    protected static final String ALL_OPTION = "all";
    protected static final String REDUCE_ON_FULL_COMPACTION_ONLY_OPTION = "reduceOnFullCompactionOnly";
    protected static final String WINDOW_SIZE = "window.size";
    @VisibleForTesting
    static final Cache<String, Boolean> loggedMsgCache = CacheBuilder.newBuilder().expireAfterWrite(1, TimeUnit.HOURS)
            .maximumSize(10000).build();

    private LookaheadIterator source = null;
    private boolean isMajorCompaction;
    private boolean reduceOnFullCompactionOnly;
    private Key topKey;
    private Value topValue;
    private Key workKey = new Key();
    private ColumnSet combiners;
    private boolean combineAllColumns;
    private long window = -1L;

    public void setSource(SortedKeyValueIterator<Key, Value> source) {
        this.source = new LookaheadIterator(source);
    }

    @Override
    public Key getTopKey() {
        return topKey;
    }

    @Override
    public Value getTopValue() {
        return topValue;
    }

    @Override
    public boolean hasTop() {
        return (null != topKey && null != topValue);
    }

    @Override
    public void next() throws IOException {
        if (topKey != null) {
            topKey = null;
            topValue = null;
        }
        source.next();

        findTop();
    }

    private void sawDelete() {
        if (isMajorCompaction && !reduceOnFullCompactionOnly) {
            try {
                loggedMsgCache.get(this.getClass().getName(), new Callable<Boolean>() {

                    @Override
                    public Boolean call() throws Exception {
                        sawDeleteLog
                                .error("Combiner of type {} saw a delete during a partial compaction.  This could cause undesired results.  See ACCUMULO-2232.  Will not log subsequent "
                                        + "occurences for at least 1 hour.", this.getClass().getSimpleName());
                        // the value is not used and does not matter
                        return Boolean.TRUE;
                    }
                });
            } catch (ExecutionException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Sets the topKey and topValue based on the top key of the source. If the
     * column of the source top key is in the set of combiners, topKey will be
     * the top key of the source and topValue will be the result of the reduce
     * method. Otherwise, topKey and topValue will be unchanged. (They are
     * always set to null before this method is called.)
     */
    private void findTop() throws IOException {
        // check if aggregation is needed
        if (source.hasTop()) {
            workKey.set(source.getTopKey());
            source.getTopValue(); // Have to eat the value in case we called
                                  // peek in the TimeWindowValueIterator
            if (combineAllColumns || combiners.contains(workKey)) {
                if (workKey.isDeleted()) {
                    sawDelete();
                    return;
                }
                topKey = workKey;
                Iterator<KeyValuePair> viter = new TimeWindowValueIterator(source, this.window);
                topValue = reduce(topKey, viter);
                // Consume the rest of the keys
                while (viter.hasNext()) {
                    viter.next();
                }
            }
        }
    }

    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        // do not want to seek to the middle of a value that should be
        // combined...
        Range seekRange = IteratorUtil.maximizeStartKeyTimeStamp(range);

        source.seek(seekRange, columnFamilies, inclusive);
        findTop();

        if (range.getStartKey() != null) {
            while (hasTop() && getTopKey().equals(range.getStartKey(), PartialKey.ROW_COLFAM_COLQUAL_COLVIS)
                    && getTopKey().getTimestamp() > range.getStartKey().getTimestamp()) {
                next();
            }

            while (hasTop() && range.beforeStartKey(getTopKey())) {
                next();
            }
        }
    }

    /**
     * Reduces a list of Values into a single Value.
     *
     * @param key
     *            The most recent version of the Key being reduced.
     *
     * @param iter
     *            An iterator over the Values for different versions of the key.
     *
     * @return The combined Value.
     */
    public abstract Value reduce(Key key, Iterator<KeyValuePair> iter);

    @Override
    public void init(SortedKeyValueIterator<Key, Value> source, Map<String, String> options, IteratorEnvironment env)
            throws IOException {
        this.source = new LookaheadIterator(source);

        if (!options.containsKey(WINDOW_SIZE)) {
            throw new IllegalArgumentException("options must include " + WINDOW_SIZE);
        }
        this.window = AccumuloConfiguration.getTimeInMillis(options.get(WINDOW_SIZE));

        combineAllColumns = false;
        if (options.containsKey(ALL_OPTION)) {
            combineAllColumns = Boolean.parseBoolean(options.get(ALL_OPTION));
            if (combineAllColumns)
                return;
        }

        if (!options.containsKey(COLUMNS_OPTION))
            throw new IllegalArgumentException("Must specify " + COLUMNS_OPTION + " option");

        String encodedColumns = options.get(COLUMNS_OPTION);
        if (encodedColumns.length() == 0)
            throw new IllegalArgumentException("The " + COLUMNS_OPTION + " must not be empty");

        combiners = new ColumnSet(Lists.newArrayList(Splitter.on(",").split(encodedColumns)));

        isMajorCompaction = env.getIteratorScope() == IteratorScope.majc;

        String rofco = options.get(REDUCE_ON_FULL_COMPACTION_ONLY_OPTION);
        if (rofco != null) {
            reduceOnFullCompactionOnly = Boolean.parseBoolean(rofco);
        } else {
            reduceOnFullCompactionOnly = false;
        }

        if (reduceOnFullCompactionOnly && isMajorCompaction && !env.isFullMajorCompaction()) {
            // adjust configuration so that no columns are combined for a
            // partial major compaction
            combineAllColumns = false;
            combiners = new ColumnSet();
        }

    }

    @Override
    public SortedKeyValueIterator<Key, Value> deepCopy(IteratorEnvironment env) {
        TimeWindowCombiner newInstance;
        try {
            newInstance = this.getClass().newInstance();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        newInstance.setSource(source.getSource().deepCopy(env));
        newInstance.combiners = combiners;
        newInstance.combineAllColumns = combineAllColumns;
        newInstance.isMajorCompaction = isMajorCompaction;
        newInstance.reduceOnFullCompactionOnly = reduceOnFullCompactionOnly;
        newInstance.window = this.window;
        return newInstance;
    }

    @Override
    public IteratorOptions describeOptions() {
        IteratorOptions io = new IteratorOptions("comb",
                "Combiners apply reduce functions to multiple versions of values with otherwise equal keys", null, null);
        io.addNamedOption(ALL_OPTION, "set to true to apply Combiner to every column, otherwise leave blank. if true, "
                + COLUMNS_OPTION + " option will be ignored.");
        io.addNamedOption(COLUMNS_OPTION,
                "<col fam>[:<col qual>]{,<col fam>[:<col qual>]} escape non-alphanum chars using %<hex>.");
        io.addNamedOption(REDUCE_ON_FULL_COMPACTION_ONLY_OPTION,
                "If true, only reduce on full major compactions.  Defaults to false. ");
        io.addNamedOption(WINDOW_SIZE, "Size (in time) of the window in which to combine values");
        return io;
    }

    @Override
    public boolean validateOptions(Map<String, String> options) {
        if (options.containsKey(ALL_OPTION)) {
            try {
                combineAllColumns = Boolean.parseBoolean(options.get(ALL_OPTION));
            } catch (Exception e) {
                throw new IllegalArgumentException("bad boolean " + ALL_OPTION + ":" + options.get(ALL_OPTION));
            }
            if (combineAllColumns)
                return true;
        }
        if (!options.containsKey(COLUMNS_OPTION))
            throw new IllegalArgumentException("options must include " + ALL_OPTION + " or " + COLUMNS_OPTION);

        String encodedColumns = options.get(COLUMNS_OPTION);
        if (encodedColumns.length() == 0)
            throw new IllegalArgumentException("empty columns specified in option " + COLUMNS_OPTION);

        for (String columns : Splitter.on(",").split(encodedColumns)) {
            if (!ColumnSet.isValidEncoding(columns))
                throw new IllegalArgumentException("invalid column encoding " + encodedColumns);
        }

        if (!options.containsKey(WINDOW_SIZE)) {
            throw new IllegalArgumentException("options must include " + WINDOW_SIZE);
        }
        return true;
    }

    /**
     * A convenience method to set which columns a combiner should be applied
     * to. For each column specified, all versions of a Key which match that
     * 
     * @{link IteratorSetting.Column} will be combined individually in each row.
     *        This method is likely to be used in conjunction with
     *        {@link ScannerBase#fetchColumnFamily(Text)} or
     *        {@link ScannerBase#fetchColumn(Text,Text)}.
     *
     * @param is
     *            iterator settings object to configure
     * @param columns
     *            a list of columns to encode as the value for the combiner
     *            column configuration
     */
    public static void setColumns(IteratorSetting is, List<IteratorSetting.Column> columns) {
        String sep = "";
        StringBuilder sb = new StringBuilder();

        for (Column col : columns) {
            sb.append(sep);
            sep = ",";
            sb.append(ColumnSet.encodeColumns(col.getFirst(), col.getSecond()));
        }

        is.addOption(COLUMNS_OPTION, sb.toString());
    }

    /**
     * A convenience method to set the "all columns" option on a Combiner. This
     * will combine all columns individually within each row.
     *
     * @param is
     *            iterator settings object to configure
     * @param combineAllColumns
     *            if true, the columns option is ignored and the Combiner will
     *            be applied to all columns
     */
    public static void setCombineAllColumns(IteratorSetting is, boolean combineAllColumns) {
        is.addOption(ALL_OPTION, Boolean.toString(combineAllColumns));
    }

    /**
     * Combiners may not work correctly with deletes. Sometimes when Accumulo
     * compacts the files in a tablet, it only compacts a subset of the files.
     * If a delete marker exists in one of the files that is not being
     * compacted, then data that should be deleted may be combined. See <a
     * href="https://issues.apache.org/jira/browse/ACCUMULO-2232"
     * >ACCUMULO-2232</a> for more information. For correctness deletes should
     * not be used with columns that are combined OR this option should be set
     * to true.
     *
     * <p>
     * When this method is set to true all data is passed through during partial
     * major compactions and no reducing is done. Reducing is only done during
     * scan and full major compactions, when deletes can be correctly handled.
     * Only reducing on full major compactions may have negative performance
     * implications, leaving lots of work to be done at scan time.
     *
     * <p>
     * When this method is set to false, combiners will log an error if a delete
     * is seen during any compaction. This can be suppressed by adjusting
     * logging configuration. Errors will not be logged more than once an hour
     * per Combiner, regardless of how many deletes are seen.
     *
     * <p>
     * This method was added in 1.6.4 and 1.7.1. If you want your code to work
     * in earlier versions of 1.6 and 1.7 then do not call this method. If not
     * set this property defaults to false in order to maintain compatibility.
     *
     * @since 1.6.5 1.7.1 1.8.0
     */

    public static void setReduceOnFullCompactionOnly(IteratorSetting is, boolean reduceOnFullCompactionOnly) {
        is.addOption(REDUCE_ON_FULL_COMPACTION_ONLY_OPTION, Boolean.toString(reduceOnFullCompactionOnly));
    }

}
