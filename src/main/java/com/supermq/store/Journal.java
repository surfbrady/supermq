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
			rf = new RandomAccessFile("D://supermq//1.data", "rw");
			rf.setLength(1024*1024); // 预分配 1M 的文件空间  
		} catch (Exception e) {
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
		rf.seek(0L);
		int messageLength = rf.readInt();
		byte[] bytes = new byte[messageLength];
		rf.read(bytes, 0, messageLength);
		return JSON.parseObject(bytes, Message.class, null);
	}
}
