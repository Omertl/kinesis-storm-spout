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

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.storm.Config;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.kinesis.stormspout.state.IKinesisSpoutStateManager;
import com.amazonaws.services.kinesis.stormspout.state.zookeeper.ZookeeperStateManager;

/**
 * Storm spout for Amazon Kinesis. The spout fetches data from Kinesis and emits a tuple for each data record.
 * 
 * Note: every spout task handles a distinct set of shards.
 */
public class KinesisSpout implements IRichSpout, Serializable {
    private static final long serialVersionUID = 7707829996758189836L;
    private static final Logger LOG = LoggerFactory.getLogger(KinesisSpout.class);

    private final InitialPositionInStream initialPosition;

    // Initialized before open
    private final KinesisSpoutConfig config;
    private final IShardListGetter shardListGetter;
    private final IShardGetterBuilder getterBuilder;
    

    // Initialized on open
    private transient SpoutOutputCollector collector;
    private transient TopologyContext context;
    private transient KinesisRecordtSplitter spoutSplitter;

    /**
     * Constructs an instance of the spout with just enough data to bootstrap the state from.
     * Construction done here is common to all spout tasks, whereas the IKinesisSpoutStateManager created
     * in activate() is task specific.
     * 
     * @param config Spout configuration.
     * @param credentialsProvider Used when making requests to Kinesis.
     * @param clientConfiguration Client configuration used when making calls to Kinesis.
     */
    public KinesisSpout(KinesisSpoutConfig config,
            AWSCredentialsProvider credentialsProvider,
            ClientConfiguration clientConfiguration) {
        this.config = config;
        KinesisHelper helper = new KinesisHelper(config.getStreamName(),
                        credentialsProvider,
                        clientConfiguration,
                        config.getRegion());
        this.shardListGetter = helper;
        this.getterBuilder =
                new KinesisShardGetterBuilder(config.getStreamName(),
                        helper,
                        config.getMaxRecordsPerCall(),
                        config.getEmptyRecordListBackoffMillis(),
                        config.getTopUpRecordListBackoffMillis(),
                        config.getTopUpRecordListThreshold());
        this.initialPosition = config.getInitialPositionInStream();
    }

    /**
     * @param config Spout configuration.
     * @param shardListGetter Used to list the shards in the stream.
     * @param getterBuilder Used for creating shard getters for a task.
     */
    KinesisSpout(final KinesisSpoutConfig config,
            final IShardListGetter shardListGetter,
            final IShardGetterBuilder getterBuilder) {
        this.config = config;
        this.shardListGetter = shardListGetter;
        this.getterBuilder = getterBuilder;
        this.initialPosition = config.getInitialPositionInStream();
    }

    @Override
    public void open(@SuppressWarnings("rawtypes") final Map conf,
            final TopologyContext spoutContext,
            final SpoutOutputCollector spoutCollector) {
        config.setTopologyName((String) conf.get(Config.TOPOLOGY_NAME));

        this.context = spoutContext;
        this.collector = spoutCollector;
        this.spoutSplitter = new KinesisRecordtSplitter(shardListGetter, getterBuilder, config, initialPosition);
        LOG.info(this + " open() called with topoConfig task index " + spoutContext.getThisTaskIndex()
                + " for processing stream " + config.getStreamName());
    }

    @Override
    public void close() {
    }

    @Override
    public void activate() {
        LOG.debug(this + " activating. Starting to process stream " + config.getStreamName());
        final int taskIndex = context.getThisTaskIndex();
        final int totalNumTasks = context.getComponentTasks(context.getThisComponentId()).size();
        spoutSplitter.Activate(taskIndex, totalNumTasks);
    }

    // A deactivated spout will not have nextTuple called on itself.
    // When the spout is deactivated, it will not continue writing the state to Zookeeper
    // periodically (it will flush on deactivate, then close the ZK connection).
    // It will however continue updating its local state in case it is activated again later
    // and needs to carry on working.
    @Override
    public void deactivate() {
        LOG.debug(this + " deactivating.");
        try {
        	spoutSplitter.Deactivate();
        } catch (Exception e) {
            LOG.warn(this + " could not deactivate stateManager.", e);
        }

    }

    @Override
    public void nextTuple() {
    	Event curr = spoutSplitter.nextEvent();
    	if (curr!=null)
    	{
    		LOG.debug("emitting cdr message " + curr.getMessageID());
    		collector.emit(spoutSplitter.deserialize(curr), curr.getMessageID());
    	}
    	else
    	{
    		 // Sleep here for a bit if there were no records to emit.
            try {
            	LOG.debug(this + " sleeping since no records to emit");
                Thread.sleep(spoutSplitter.getEmptyRecordListSleepTime());
            } catch (InterruptedException e) {
                LOG.debug(this + " sleep was interrupted.");
            }
    	}
    }


    @Override
    public void ack(Object msgId) {
    	assert msgId instanceof String : "Expecting msgId_ to be a String";
    	spoutSplitter.ack((String) msgId);
    }

    @Override
    public void fail(Object msgId) {
    	assert msgId instanceof String : "Expecting msgId_ to be a String";
    	spoutSplitter.fail((String) msgId);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(config.getScheme().getOutputFields());
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE).append("taskIndex",
                context.getThisTaskIndex()).toString();
    }
}
