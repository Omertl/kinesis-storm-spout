/*
 * Copyright 2013-2014 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Amazon Software License (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 * http://aws.amazon.com/asl/
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazonaws.services.kinesis.stormspout;

import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.kinesis.stormspout.exceptions.InvalidSeekPositionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Allows users to do efficient getter.getNext(1) calls in exchange for maybe pulling
 * more data than necessary from Kinesis.
 */
class BufferedGetter implements IShardGetter {
    private static final Logger LOG = LoggerFactory.getLogger(BufferedGetter.class);
    private final IShardGetter getter;
    private final int maxBufferSize;
    private final long emptyRecordListBackoffTime;
    private final long topUpRecordListBackoffTime;
    private final long topUpRecordListThreshold;
    private long nextRebufferTime = 0L;
    private final TimeProvider timeProvider;

    /**
     * This buffer uses a concurrent queue to handle background prefetch
     * while current contents can be iterated on. The main thread will only
     * access items at the front of the queue, while the background task fetching
     * new records will only append new record lists to the back.
     * isEndOfShard and bufferSize are atomic types and hold whether we've reached
     * the end of the shard and the number of records remaining in the buffer,
     * respectively.
     */
    private ConcurrentLinkedQueue<Records> buffer;
    private AtomicBoolean isEndOfShard;
    private AtomicInteger bufferSize;

    private ExecutorService taskPool;

    /**
     * Creates a (shard) getter that buffers records.
     * 
     * @param underlyingGetter Unbuffered shard getter.
     * @param maxBufferSize Max number of records to fetch from the underlying getter.
     * @param emptyRecordListBackoffMillis Backoff time between GetRecords calls if previous call fetched no records.
     */
    public BufferedGetter(final IShardGetter underlyingGetter,
                          final int maxBufferSize,
                          final long emptyRecordListBackoffMillis,
                          final long topUpRecordListBackoffMillis,
                          final long topUpRecordListThreshold) {
        this(underlyingGetter,
                maxBufferSize,
                emptyRecordListBackoffMillis,
                topUpRecordListBackoffMillis,
                topUpRecordListThreshold,
                new TimeProvider());
    }
    
    /**
     * Used for unit testing.
     * 
     * @param underlyingGetter Unbuffered shard getter
     * @param maxBufferSize Max number of records to fetch from the underlying getter
     * @param emptyRecordListBackoffMillis Backoff time between GetRecords calls if previous call fetched no records.
     * @param timeProvider Useful for testing timing based behavior (e.g. backoff)
     */
    BufferedGetter(final IShardGetter underlyingGetter,
            final int maxBufferSize,
            final long emptyRecordListBackoffMillis,
            final long topUpRecordListBackoffMillis,
            final long topUpRecordListThreshold,
            final TimeProvider timeProvider) {
        this.getter = underlyingGetter;
        this.maxBufferSize = maxBufferSize;
        this.emptyRecordListBackoffTime = emptyRecordListBackoffMillis;
        this.topUpRecordListBackoffTime = topUpRecordListBackoffMillis;
        this.topUpRecordListThreshold = topUpRecordListThreshold;
        this.timeProvider = timeProvider;
        this.isEndOfShard = new AtomicBoolean(false);
        this.taskPool = Executors.newSingleThreadExecutor();
        this.buffer = new ConcurrentLinkedQueue<>();
        this.bufferSize = new AtomicInteger(0);
    }

    @Override
    public Records getNext(int maxNumberOfRecords) {
        ensureBuffered();

        if (buffer.isEmpty() && isEndOfShard.get()) {
            return new Records(ImmutableList.of(), true);
        }

        ImmutableList.Builder<Record> recs = new ImmutableList.Builder<>();
        int recsSize = 0;

        while (recsSize < maxNumberOfRecords) {
            if ((buffer != null) && !buffer.isEmpty()) {
                Optional<Record> record = buffer.peek().getNext();
                if (record.isPresent()) {
                    bufferSize.decrementAndGet();
                    recs.add(record.get());
                    recsSize++;
                } else {
                    // this Records iterator has exhausted the list
                    buffer.remove();
                }
            } else {
                // No more records
                break;
            }
        }

        return new Records(recs.build(), false);
    }

    @Override
    public void seek(ShardPosition position) throws InvalidSeekPositionException {
        getter.seek(position);
        buffer.clear();
        bufferSize.set(0);
    }

    @Override
    public String getAssociatedShard() {
        return getter.getAssociatedShard();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE).append("getter", getter.toString())
                .toString();
    }

    private void ensureBuffered() {
        if (   buffer.isEmpty()
            || (   (bufferSize.get() < topUpRecordListThreshold)
                && (timeProvider.getCurrentTimeMillis() >= nextRebufferTime))) {

            taskPool.execute(this::rebuffer);
        }
    }

    private void rebuffer() {
        if (   buffer.isEmpty()
            || (   (bufferSize.get() < topUpRecordListThreshold)
                && (timeProvider.getCurrentTimeMillis() >= nextRebufferTime))) {

            nextRebufferTime = timeProvider.getCurrentTimeMillis() + topUpRecordListBackoffTime;

            try {
                Records records = getter.getNext(maxBufferSize);
                if (!records.isEmpty()) {
                    buffer.add(records);
                    bufferSize.getAndAdd(records.getRecords().size());
                }
                isEndOfShard.set(records.isEndOfShard());
            } catch (IllegalArgumentException e) {
                LOG.debug("[" + getAssociatedShard() + "] Exception caught: ", e);
            }

            // Backoff if we get an empty record list
            if (buffer.isEmpty()) {
                nextRebufferTime = timeProvider.getCurrentTimeMillis() + emptyRecordListBackoffTime;
            }
        }
    }

    /** 
     * Time provider - helpful for unit tests of BufferedGetter.
     */
    static class TimeProvider {

        long getCurrentTimeMillis() {
            return System.currentTimeMillis();
        }
    }
}
