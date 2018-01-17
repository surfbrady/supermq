package com.supermq.boker;

import java.util.List;

import com.alibaba.fastjson.JSON;
import com.supermq.command.Message;
import com.supermq.consumer.Consumer;
import com.supermq.store.IndexLog;
import com.supermq.store.Journal;
import com.supermq.store.MsgLocation;
import com.supermq.store.SuperDB;

public class Topic {

	private List<Consumer> consumer;
	private String topicName;
	private SuperDB superDB;
	
	public Topic (String topicName, SuperDB superDB) {
		this.topicName = topicName;
		this.superDB = superDB;
	}
	
	public void addMessage(Message msg) throws Exception {
		// 添加消息
		superDB.addMessage(msg);
		
		
	}
	
	public Message getMessage() throws Exception {
		// 获取消息
		return null;
	}
	
	public static void main(String[] args) {

	}
}
