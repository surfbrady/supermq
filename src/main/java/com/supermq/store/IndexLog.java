package com.supermq.store;

import java.io.RandomAccessFile;

import com.supermq.entity.Message;

/**
 * B树的概念
 * 树中每个结点最多含有m个孩子（m>=2）；
 *     除根结点和叶子结点外，其它每个结点至少有[ceil(m / 2)]个孩子（其中ceil(x)是一个取上限的函数）；
 *    若根结点不是叶子结点，则至少有2个孩子（特殊情况：没有孩子的根结点，即根结点为叶子结点，整棵树只有一个根节点）；
 *    所有叶子结点都出现在同一层，叶子结点不包含任何关键字信息(可以看做是外部接点或查询失败的接点，实际上这些结点不存在，指向这些结点的指针都为null)；
 *     每个非终端结点中包含有n个关键字信息： (n，P0，K1，P1，K2，P2，......，Kn，Pn)。其中：
 *      a)   Ki (i=1...n)为关键字，且关键字按顺序升序排序K(i-1)< Ki。 
 *      b)   Pi为指向子树根的接点，且指针P(i-1)指向子树种所有结点的关键字均小于Ki，但都大于K(i-1)。 
 *      c)   关键字的个数n必须满足： [ceil(m / 2)-1]<= n <= m-1。
 * 
 * 
 * 
 * 
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
		String topicName = message.get
		// 第一步找到root节点
		if () {
			
		}
		// 第二步找到插入位置，查看是否需要分裂节点
	}
	
	public static class MsgLocation {
		private long messageId;
		private long location;
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
		
	}
}
