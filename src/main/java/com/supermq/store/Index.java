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
public class Index {

	RandomAccessFile rf;
	
	public Index() {
		// 现在是方便测试用，每次是新建一个50M的文件
		try {
			rf = new RandomAccessFile("D://supermq//1.data", "rw");
			rf.setLength(1024*1024*50); // 预分配 50M 的文件空间  
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void addMessage(Message message) {
		String topicName = message.get
		// 第一步找到root节点
		
		// 第二步找到插入位置，查看是否需要分裂节点
	}
}
