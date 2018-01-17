package com.supermq.command;

import java.io.Serializable;

public class Message implements Command, Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1173318888346227200L;
	
	private long messageId;
	private String context;
	private Destination destination;
	
	public long getMessageId() {
		return messageId;
	}
	public void setMessageId(long messageId) {
		this.messageId = messageId;
	}
	public String getContext() {
		return context;
	}
	public void setContext(String context) {
		this.context = context;
	}
	public Destination getDestination() {
		return destination;
	}
	public void setDestination(Destination destination) {
		this.destination = destination;
	}
	public boolean isMessage() {
		return true;
	}
	
}
