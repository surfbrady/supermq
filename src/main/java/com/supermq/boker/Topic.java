package com.supermq.boker;

import java.util.List;

import com.alibaba.fastjson.JSON;
import com.supermq.consumer.Consumer;
import com.supermq.entity.Message;
import com.supermq.store.IndexLog;
import com.supermq.store.Journal;

public class Topic {

	private List<Consumer> consumer;
	private String topicName;
	private Journal journal;
	
	public Topic (String topicName, Journal journal) {
		this.topicName = topicName;
		this.journal = journal;
	}
	
	public void addMessage(Message msg) throws Exception {
		// 添加消息
		IndexLog.MsgLocation msgLocation = journal.addMessage(msg);
		
		
	}
	
	public Message getMessage() throws Exception {
		// 获取消息
		return journal.getMessage();
	}
	
	public static void main(String[] args) {
		Message msg = new Message();
		msg.setContext("yes first");
		Topic topic = new Topic("aa", new Journal());
		try {
			topic.addMessage(msg);
			System.out.println(JSON.toJSONString(topic.getMessage()));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
