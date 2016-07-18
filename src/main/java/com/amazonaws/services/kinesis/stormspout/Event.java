package com.amazonaws.services.kinesis.stormspout;

import java.util.Arrays;

/**
 * This class represents a single event
 * @author omer
 *
 */
public class Event {
	/**
	 * Constructor
	 * @param data - the record itself
	 * @param index - the index inside the collection
	 * @param shardId
	 * @param sequenceNumber
	 * @param partitionKey
	 */
	public Event(String data, long index, String shardId, String sequenceNumber, String partitionKey) {
		super();
		this.data = data;
		this.index = index;
		this.retries = 0;
		this.shardId = shardId;
		this.sequenceNumber = sequenceNumber;
		this.partitionKey = partitionKey;
		this.getKinesisRetries();
	}
	
	private void getKinesisRetries()
	{
		byte[] currData = data.getBytes();
		if (currData[0] == 0xF)
		{
			this.KinesisRetry = currData[1];
			this.data = Arrays.copyOfRange(currData, 0, 2).toString();
		}
		else
			this.KinesisRetry = 0;
	}
	
	public void addKinesisRetry()
	{
		this.KinesisRetry++;
		byte[] newRetry = new byte[2];
		newRetry[0] = 0xF;
		newRetry[1] = this.KinesisRetry;
		this.data = newRetry.toString().concat(data);
	}
	
	public String getData() {
		return data;
	}
	
	public void retry()
	{
		retries++;
	}
	public void setData(String data) {
		this.data = data;
	}
	public long getIndex() {
		return index;
	}
	public void setIndex(long index) {
		this.index = index;
	}
	public int getRetries() {
		return retries;
	}
	public void setRetries(int retries) {
		this.retries = retries;
	}
	
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
	
	private String data;
	private long index;
	private int retries;
	private String shardId;
	private String sequenceNumber;
	private String partitionKey;
	private byte KinesisRetry;
	
	public String getPartitionKey() {
		return partitionKey;
	}
	public void setPartitionKey(String partitionKey) {
		this.partitionKey = partitionKey;
	}
	
	/**
	 * Get the message id - shard, sequence and index
	 * @return
	 */
	public String getMessageID()
	{
		return MessageIdUtil.constructEventId(shardId, sequenceNumber, index);
	}
}
