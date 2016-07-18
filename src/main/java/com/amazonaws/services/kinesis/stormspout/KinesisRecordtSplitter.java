package com.amazonaws.services.kinesis.stormspout;

import java.io.Serializable;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.kinesis.stormspout.state.IKinesisSpoutStateManager;
import com.amazonaws.services.kinesis.stormspout.state.zookeeper.ZookeeperStateManager;
import com.google.common.collect.ImmutableList;

/**
 * Handling the spout tasks while splitting records
 * @author omer
 *
 */
public class KinesisRecordtSplitter implements Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(KinesisRecordtSplitter.class);
    
	private Map<String, EventCollection> event_collections;
	private long emptyRecordListSleepTimeMillis = 5L;
	
	private final KinesisSpoutConfig config;
	private final IShardListGetter getter;
	
    private transient CharsetDecoder decoder;
	private transient IKinesisSpoutStateManager stateManager;
	private transient long lastCommitTime;
	
	/**
	 * Constructor
	 * @param getter Used to list the shards in the stream.
	 * @param getterBuilder Used for creating shard getters for a task.
	 * @param conf Spout configuration.
	 * @param initialPosition in stream
	 */
	public KinesisRecordtSplitter(IShardListGetter getter,IShardGetterBuilder getterBuilder,KinesisSpoutConfig conf,InitialPositionInStream initialPosition )
	{
		this.stateManager = new ZookeeperStateManager(conf, getter, getterBuilder, initialPosition);
		this.event_collections = new HashMap<>();
		decoder = Charset.forName("UTF-8").newDecoder();
		this.getter = getter;
		this.config = conf;
	}
	
	/**
	 * Activates the spout
	 * @param taskIndex
	 * @param totalNumTasks
	 */
	public void Activate(int taskIndex,int totalNumTasks)
	{
		stateManager.activate();
        stateManager.rebalance(taskIndex, totalNumTasks);
        lastCommitTime = System.currentTimeMillis();
	}
	
	/**
	 * Deactivates the spout
	 * @throws Exception
	 */
	public void Deactivate() throws Exception
	{
		stateManager.deactivate();
	}
	
	/**
	 * Get the next splitted Event row
	 * @return
	 */
	public Event nextEvent()
	{
    	LOG.debug("asked for next tuple");
		boolean found = false;
		// Get the list of active collections
		Iterator<EventCollection> iterator = event_collections.values().iterator();
		EventCollection c = null;
		// Go over all the event collections until finding one with records to process in queue
		while (iterator.hasNext() && !found)
		{
			c = iterator.next();
			// Check if this collection has more records waiting for process
			if (!c.noMoreToProcess())
			{
				LOG.debug(this + " found event collection " + c.getKey());
				found=true;
			}
		}
		// If there aren't any collections with records to be processes - getting some more from Kinesis
		if (!found)
			c = nextRecord();
		Event curr = null;
		// If there's a collection with records in it
		if (c!=null)
		{
			// Get the next event to process
			curr = c.getEvent_to_process();
			LOG.debug(this + " next event to process " + curr.getMessageID());
			// Move this event to the in process list
			c.setEvent_in_process(curr, curr.getIndex());	
			LOG.debug(this + " event data " + curr.getData());
		}
		else
		{
			LOG.debug(this + " no event to process");
		}
		return curr;
	}
	
	/**
	 * Deserialize the current event so can be used in tuple
	 * @param event
	 * @return
	 */
    public List<Object> deserialize(Event event) {
        final List<Object> l = new ArrayList<>();
        l.add(event.getPartitionKey());
        l.add(event.getData());
        return l;
    }
    
    /**
     * Called in case a event record is acked
     * @param msgId
     */
    public void ack(String msgId)
    {
    	synchronized (stateManager) {
        	LOG.debug(this + " called to ack " + msgId);
    		// Get the metadata of the event
        	String shard = MessageIdUtil.shardIdOfMessageId(msgId);
        	String seq = MessageIdUtil.sequenceNumberOfMessageId(msgId);
        	Long index = Long.parseLong(MessageIdUtil.inexOfMessageId(msgId));
        	Object collectionId = MessageIdUtil.constructMessageId(shard, seq);
        	// Get the relevant collection
        	EventCollection coll = event_collections.get(collectionId);
        	// Get the relevant event and remove it from the in process list
        	coll.getEvent_in_process(index);
        	// If all the events in the collection are acked, ack the state manager
        	if (coll.allAcked())
        	{
        		if (LOG.isDebugEnabled()) {
                    LOG.debug(this + " Processing ack() for " + msgId + ", shardId " + shard + " seqNum " + seq);
                }
        		// Remove the collection from the collection list
        		event_collections.remove(collectionId);
                stateManager.ack(shard, seq);
        	}
    	}
    }
    
    /**
     * Called in case a event record is failed
     * @param msgId
     */
    public void fail(String msgId)
    {
    	synchronized (stateManager) {
        	LOG.debug(this + " called to fail " + msgId);
    		// Get the metadata for the event
    		String shard = MessageIdUtil.shardIdOfMessageId(msgId);
        	String seq = MessageIdUtil.sequenceNumberOfMessageId(msgId);
        	Long index = Long.parseLong(MessageIdUtil.inexOfMessageId(msgId));
        	Object collectionId = MessageIdUtil.constructMessageId(shard, seq);
        	// Find the relevant event collection
        	EventCollection coll = event_collections.get(collectionId);
        	// Remove the event from the list in the collection
        	Event event = coll.getEvent_in_process(index);
        	// Increase the number of retries
        	event.retry();
        	// If this was the last retry
        	if (event.getRetries()==config.getRecordRetryLimit())
        	{
            	LOG.debug(this + " this is the last retry for " + msgId);
        		// Checking if we can use kinesis helper to write the record back to the stream
        		if (this.getter instanceof KinesisHelper)
        		{
                	LOG.debug(this + " pushin to kinesis " + msgId);
                	// Increase the retry indication inside the event
                	event.addKinesisRetry();
        			// Put the event record back into the stream
        			((KinesisHelper) this.getter).putRecords(event.getPartitionKey(), event.getData());
        		}
        		// If every other event in the collection is acked
        		if (coll.allAcked())
        		{
            		if (LOG.isDebugEnabled()) {
                        LOG.debug(this + " Processing fail() for " + msgId + ", shardId " + shard + " seqNum " + seq);
                    }
            		// Remove the collection and send ack to the state manager
            		event_collections.remove(collectionId);
                    stateManager.ack(shard, seq);
        		}
        	}
        	else
        	{
            	LOG.debug(this + " retrying for " + msgId);
        		// Put the event back into the to process list for another retry
        		coll.setEvent_to_process(event);
        	}
    	}
    }
	
    /**
     * Get next record from the kinesis stream
     * @return
     */
	private EventCollection nextRecord()
	{
		synchronized (stateManager) {
			// Task has no assignments.
            if (!stateManager.hasGetters()) {
                // Sleep here for a bit, so we don't consume too much cpu.
                try {
                	LOG.debug(this + " sleeping for " + emptyRecordListSleepTimeMillis);
                    Thread.sleep(emptyRecordListSleepTimeMillis);
                } catch (InterruptedException e) {
                    LOG.debug(this + " sleep was interrupted.");
                }
                return null;
            }
            
            LOG.debug(this + " getting new record from kinesis");
            
            final IShardGetter getter = stateManager.getNextGetter();
            String currentShardId = getter.getAssociatedShard();
            Record rec = null;
            EventCollection new_collection = null;
            // Get the next record
            final ImmutableList<Record> records = getter.getNext(1).getRecords();
            if ((records != null) && (!records.isEmpty())) {
                rec = records.get(0);
                String data;
				try {
					// Build event collection from the record
					data = decoder.decode(rec.getData()).toString();
					String seqNum = rec.getSequenceNumber();
					String partitionKey = rec.getPartitionKey();
					new_collection = new EventCollection(currentShardId, seqNum, partitionKey);
					// Split the record into multiple events by newline
					LOG.debug(this + " sending to split " + new_collection.getKey());
					new_collection.splitter(data, config.getDelimiter());
					// Put this new event collection into the collection list
					this.event_collections.put(new_collection.getKey(), new_collection);
					// Send this record as emitted to the state manager
	                stateManager.emit(currentShardId, rec, false);
				} catch (CharacterCodingException e) {
					LOG.debug(this + " encountered coding exception");
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
            }
            else
            {
            	// Sleep here for a bit if there were no records to emit.
                try {
                	LOG.debug(this + " sleeping for " + emptyRecordListSleepTimeMillis);
                    Thread.sleep(emptyRecordListSleepTimeMillis);
                } catch (InterruptedException e) {
                    LOG.debug(this + " sleep was interrupted.");
                }
            }
          // Do periodic ZK commit of shard states.
          if (System.currentTimeMillis() - lastCommitTime >= config.getCheckpointIntervalMillis()) {
              LOG.debug(this + " committing local shard states to ZooKeeper.");

              stateManager.commitShardStates();
              lastCommitTime = System.currentTimeMillis();
          } else {
              LOG.debug(this + " Not committing to ZooKeeper.");
          }
          return new_collection;
		}
	}
	
	public long getEmptyRecordListSleepTime()
	{
		return emptyRecordListSleepTimeMillis;
	}
}
