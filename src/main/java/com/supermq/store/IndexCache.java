package com.supermq.store;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.collections.CollectionUtils;

import com.supermq.entity.Message;
import com.supermq.store.IndexLog.MsgLocation;

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
 * @author brady
 *
 */
public class IndexCache {

	private static ConcurrentHashMap<String, BTreeNode> bTreeMap = new ConcurrentHashMap<String, BTreeNode>();
	
	private static final int m = 50;       // 50阶B树
	
	public IndexCache() {
		
	}
	
	public void addMessage(Message message) throws Exception {
		String topicName = message.getDestination().getName();
		// 首先往日志中插入节点
		MsgLocation msgLocation = new MsgLocation();
		msgLocation.setMessageId(message.getMessageId());
		
		// 第一步找到root节点
		BTreeNode rootNode = bTreeMap.get(topicName);
		if (rootNode==null) {
			rootNode = new BTreeNode();
			BTreeNode btreeNode = bTreeMap.putIfAbsent(topicName, rootNode);
			if (btreeNode!=null) {
				rootNode = btreeNode;
			}
		}
		// 第二步找到插入位置
		BTreeNode insertNode = findInsertNode(rootNode, msgLocation);
		// 第三步插入节点
		insertKeyInBTreeNode(insertNode, msgLocation);
		// 第四步判断节点是否需要分裂
		divideNode(insertNode);
	}

	/**
	 * 判断是否需要分裂节点
	 * @param insertNode
	 */
	private void divideNode(BTreeNode insertNode) {
		// 节点未满
		if (insertNode.getKeys().size() < m) {
			return;
		}
		
		// 如果是根节点
		if (insertNode.getParent()==null) {
			BTreeNode rootNode = new BTreeNode();
			insertNode.setParent(rootNode);
		}
		
		// 节点已满，需要将中间关键字，移入父节点
		List<MsgLocation> keys = insertNode.getKeys();
		insertKeyInBTreeNode(insertNode.getParent(), keys.get(keys.size()/2));
	}

	/**
	 * 在节点中插入关键字
	 * @param insertNode
	 * @param message
	 */
	private void insertKeyInBTreeNode(BTreeNode insertNode, MsgLocation msgLocation) {
		List<MsgLocation> keys = insertNode.getKeys();
		
		// 遍历查找
		for(int i=0; i<keys.size(); i++) {
			if (keys.get(i).getMessageId()>msgLocation.getMessageId()) {
				keys.add(i-1, msgLocation);
				return;
			}
		}
		keys.add(keys.size(), msgLocation);
	}

	/**
	 * 查找应该插入节点的位置
	 * @param rootNode
	 * @param message
	 */
	private BTreeNode findInsertNode(BTreeNode searchNode, MsgLocation msgLocation) throws Exception {
		if (CollectionUtils.isEmpty(searchNode.getChilds()) || CollectionUtils.isEmpty(searchNode.getKeys())) {
			return searchNode;
		}
		
		List<MsgLocation> keys = searchNode.getKeys();
		// 遍历查找
		for(int i=0; i<keys.size(); i++) {
			if (keys.get(i).getMessageId()>msgLocation.getMessageId()) {
				return findInsertNode(searchNode.getChilds().get(i), msgLocation);
			}
		}
		
		throw new Exception("找到插入位置");
	}
	
	/**
	 * 打印B树的所有节点
	 * @param topicName
	 * @return
	 */
	private String printNode(String topicName) {
		BTreeNode rootNode = bTreeMap.get(topicName);
		if (rootNode==null) {
			return null;
		}
		
		return printNode(rootNode);
	}

	private String printNode(BTreeNode bTreeNode) {
		StringBuilder sb = new StringBuilder();
		for (int i=0; i<bTreeNode.getKeys().size(); i++) {
			 // 先打印左子树，
			 if (CollectionUtils.isNotEmpty(bTreeNode.getChilds()) && bTreeNode.getChilds().size()>=i) {
				 sb.append(printNode(bTreeNode.getChilds().get(i))+",");
			 }
			 // 打印该节点
			 sb.append(bTreeNode.getKeys().get(i).getMessageId()+",");
			 // 在打印右子树
			 if (CollectionUtils.isNotEmpty(bTreeNode.getChilds()) && bTreeNode.getChilds().size()>=i+1) {
				 sb.append(printNode(bTreeNode.getChilds().get(i+1))+",");
			 }
			
		}
		
		return sb.toString();
	}
	
}
