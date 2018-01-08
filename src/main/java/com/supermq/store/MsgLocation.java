package com.supermq.store;

/**
 * 消息存储在日志中的位置
 * @author brady
 *
 */
public class MsgLocation {
	
	private long messageId;
	private long location;
	private int size;
	public long getMessageId() {
		return messageId;
	}
	public void setMessageId(long messageId) {
		this.messageId = messageId;
	}
	public long getLocation() {
		return location;
	}
	public void setLocation(long location) {
		this.location = location;
	}
	public int getSize() {
		return size;
	}
	public void setSize(int size) {
		this.size = size;
	}
}
