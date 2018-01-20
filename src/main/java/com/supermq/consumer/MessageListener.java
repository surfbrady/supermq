package com.supermq.consumer;

import java.util.List;

import com.supermq.command.Message;

public interface MessageListener {
	
	public void onMessage(List<Message> messages);
	
}
