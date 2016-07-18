package com.amazonaws.services.kinesis.stormspout;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

/**
 * This class represents a collection of events. It's derived from a Kinesis record
 * @author omer
 *
 */
public class EventCollection {
	public String getShardId() {
		return shardId;
	}
	public void setShardId(String shardId) {
		this.shardId = shardId;
	}
	public String getSequenceNumber() {
		return sequenceNumber;
	}
	public void setSequenceNumber(String sequenceNumber) {
		this.sequenceNumber = sequenceNumber;
	}
	public String getPartitionKey() {
		return partitionKey;
	}
	public void setPartitionKey(String partitionKey) {
		this.partitionKey = partitionKey;
	}
	public void setEvent_to_process(Event event)
	{
		this.events_to_process.add(event);
	}
	public Event getEvent_to_process()
	{
		return this.events_to_process.remove();
	}
	public void setEvent_in_process(Event event, Long index)
	{
		this.events_in_process.put(index, event);
	}
	public Event getEvent_in_process(Long index)
	{
		return events_in_process.remove(index);
	}
	/**
	 * Check if there are events being processed or waiting to be processed
	 * @return true if all is acked and false otherwise
	 */
	public boolean allAcked()
	{
		if (events_in_process.isEmpty() && events_to_process.isEmpty())
			return true;
		else
			return false;
	}
	private Queue<Event> events_to_process;
	private Map<Long, Event> events_in_process;
	private String shardId;
	private String sequenceNumber;
	private String partitionKey;	
	/**
	 * Constructor. Creates a new list and submits the shard, sequence and partition parameters
	 * @param shardId
	 * @param seqNum
	 * @param Partition
	 */
	public EventCollection(String shardId,String seqNum, String Partition)
	{
		this.setPartitionKey(Partition);
		this.setSequenceNumber(seqNum);
		this.setShardId(shardId);
		this.events_in_process = new HashMap<>();
		this.events_to_process = new LinkedList<>();
	}
	
	/**
	 * Checks if there are more events waiting to be processed in the queue 
	 * @return
	 */
	public boolean noMoreToProcess()
	{
		return events_to_process.isEmpty();
	}
	
	/**
	 * Split a string into the queue by a delimiter 
	 * @param input
	 * @param splitBy
	 */
	public void splitter(String input,String splitBy)
	{
		String[] splitted = input.split(splitBy);
		long counter=0;
		for (String curr:splitted)
		{
			counter++;
			this.events_to_process.add(new Event(curr,counter,shardId,sequenceNumber,partitionKey));
		}
	}
	
	/**
	 * Returns the collection key - shard and sequence
	 * @return
	 */
	public String getKey()
	{
		return shardId + ":" + sequenceNumber;
	}
}
