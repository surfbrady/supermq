package com.supermq.store;

import java.io.RandomAccessFile;

import com.alibaba.fastjson.JSON;
import com.supermq.command.Message;

/**
 * 日志文件用于记录消息
 * 
 * @author brady
 *
 */
public class Journal {
	RandomAccessFile rf;
	int offset;
	
	public Journal () {
		try {
			rf = new RandomAccessFile("D://supermq//1.log", "rw");
			rf.setLength(1024*1024*50); // 预分配 50M 的文件空间  
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public MsgLocation addMessage(Message msg) throws Exception {
		// 添加消息
		byte[] bytes = JSON.toJSONString(msg).getBytes();
		long start = rf.getFilePointer();
		rf.writeInt(bytes.length);
		rf.write(bytes);
		MsgLocation msgLocation = new MsgLocation();
		msgLocation.setSize(4 + bytes.length);
		msgLocation.setMessageId(msg.getMessageId());
		msgLocation.setLocation(start);
		return msgLocation;
	}

	public Message getMessage() throws Exception {
		rf.seek(0L);
		int messageLength = rf.readInt();
		byte[] bytes = new byte[messageLength];
		rf.read(bytes, 0, messageLength);
		return JSON.parseObject(bytes, Message.class, null);
	}
	
	
}
