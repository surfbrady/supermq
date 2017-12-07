package com.supermq.store;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

import com.alibaba.fastjson.JSON;
import com.supermq.entity.Message;

/**
 * 日志文件用于记录消息
 * @author brady
 *
 */
public class Journal {
	RandomAccessFile rf;
	int offset;
	
	public Journal () {
		try {
			RandomAccessFile rf = new RandomAccessFile("D://supermq//1.data", "rw");
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}
	
	public void addMessage(Message msg) throws Exception {
		// 添加消息
		byte[] bytes = JSON.toJSONString(msg).getBytes();
		rf.writeInt(bytes.length);
		rf.write(bytes);
	}

	public Message getMessage() throws Exception {
		int messageLength = rf.readInt();
		byte[] bytes = null;
		rf.read(bytes, 4, messageLength);
		return JSON.parseObject(bytes, Message.class, null);
	}
}
