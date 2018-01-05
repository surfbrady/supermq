package com.supermq.store;

import java.io.RandomAccessFile;

import com.supermq.entity.Message;

/**
 * 读写索引文件
 * 目前使用B树进行存储
 * 文件分两部分，第一部分指出都有哪些B树，对应的root节点地址是什么，分配一页，即4KB
 * 第二部分是B树部分。以4KB为一页
 * 
 * @author brady
 *
 */
public class IndexLog {

	RandomAccessFile rf;
	
	public IndexLog() {
		// 现在是方便测试用，每次是新建一个50M的文件
		try {
			rf = new RandomAccessFile("D://supermq//1.data", "rw");
			rf.setLength(1024*1024*50); // 预分配 50M 的文件空间  
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void addMessage(Message message) {
		
	}
	
	public static class MsgLocation {
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
}
