package com.supermq.store;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;

import com.alibaba.fastjson.JSON;
import com.supermq.entity.Destination;
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
 * B树的插入:
 * 
 * 
 * 在B-树上删除关键字k的过程分两步完成：
 *   （1）利用前述的B-树的查找算法找出该关键字所在的结点。然后根据 k所在结点是否为叶子结点有不同的处理方法。
 *   （2）若该结点为非叶结点，且被删关键字为该结点中第i个关键字key[i]，则可从指针son[i]所指的子树中找出最小关键字Y，代替key[i]的位置，然后在叶结点中删去Y。
 *   
 * 因此，把在非叶结点删除关键字k的问题就变成了删除叶子结点中的关键字的问题了
 * 
 * 在B-树叶结点上删除一个关键字的方法是
 * 首先将要删除的关键字 k直接从该叶子结点中删除。然后根据不同情况分别作相应的处理，共有三种可能情况：
 * （1）如果被删关键字所在结点的原关键字个数n>=ceil(m/2)，说明删去该关键字后该结点仍满足B-树的定义。这种情况最为简单，只需从该结点中直接删去关键字即可。
 * （2）如果被删关键字所在结点的关键字个数n等于ceil(m/2)-1，说明删去该关键字后该结点将不满足B-树的定义，需要调整。
 * 调整过程为：如果其左右兄弟结点中有“多余”的关键字,即与该结点相邻的右（左）兄弟结点中的关键字数目大于ceil(m/2)-1。则可将右（左）兄弟结点中最小（大）关键字上移至双亲结点。而将双亲结点中小（大）于该上移关键字的关键字下移至被删关键字所在结点中。
 * （3）如果左右兄弟结点中没有“多余”的关键字，即与该结点相邻的右（左）兄弟结点中的关键字数目均等于ceil(m/2)-1。这种情况比较复杂。需把要删除关键字的结点与其左（或右）兄弟结点以及双亲结点中分割二者的关键字合并成一个结点,即在删除关键字后，该结点中剩余的关键字加指针，加上双亲结点中的关键字Ki一起，合并到Ai（是双亲结点指向该删除关键字结点的左（右）兄弟结点的指针）所指的兄弟结点中去。如果因此使双亲结点中关键字个数小于ceil(m/2)-1，则对此双亲结点做同样处理。以致于可能直到对根结点做这样的处理而使整个树减少一层。
 * 总之，设所删关键字为非终端结点中的Ki，则可以指针Ai所指子树中的最小关键字Y代替Ki，然后在相应结点中删除Y。对任意关键字的删除都可以转化为对最下层关键字的删除
 * 
 * @author brady
 *
 */
public class IndexCache {

	private static ConcurrentHashMap<String, BTreeNode> bTreeMap = new ConcurrentHashMap<String, BTreeNode>();
	
	private static final int m = 4;       // 50阶B树
	
	public IndexCache() {
		
	}
	
	public void addMessageIndex(MsgLocation msgLocation, Message message) throws Exception {
		String topicName = message.getDestination().getName();
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
		
		while (rootNode.getParent()!=null) {
			rootNode = rootNode.getParent();
		}
		bTreeMap.put(topicName, rootNode);
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
		BTreeNode newNode = new BTreeNode();
		newNode.setParent(insertNode.getParent());
		int middle = insertNode.getKeys().size()/2;
		// 遍历查找
		for(int i=0; i<middle; i++) {
			newNode.getKeys().add(insertNode.getKeys().get(0));
			insertNode.getKeys().remove(0);
			// 如果非叶子结点
			if (CollectionUtils.isNotEmpty(insertNode.getChilds())) {
				newNode.getChilds().add(insertNode.getChilds().get(0));
				insertNode.getChilds().get(0).setParent(newNode);
				insertNode.getChilds().remove(0);
			}
		}
		
		if (CollectionUtils.isNotEmpty(insertNode.getChilds())) {
			newNode.getChilds().add(insertNode.getChilds().get(0));
			insertNode.getChilds().get(0).setParent(newNode);
			insertNode.getChilds().remove(0);
		}
		
		// 在父结点中插入关键字和孩子
		int insertLocation = -1;
		for (int i=0; i<insertNode.getParent().getKeys().size(); i++) {
			if (insertNode.getParent().getKeys().get(i).getMessageId()>insertNode.getKeys().get(0).getMessageId()) {
				insertLocation = i;
				break;
			}
		}
		
		BTreeNode parent = insertNode.getParent();
		if (insertLocation==-1) {
			insertLocation = (parent.getKeys().size()==0)?0:(parent.getKeys().size());
		}
		parent.getKeys().add(insertLocation, insertNode.getKeys().get(0));
		insertNode.getKeys().remove(0);
		parent.getChilds().add(insertLocation, newNode);
		parent.getChilds().add(insertLocation+1, insertNode);

		
 		divideNode(parent);
	}

	/**
	 * 在节点中插入关键字
	 * @param insertNode
	 * @param message
	 */
	private void insertKeyInBTreeNode(BTreeNode insertNode, MsgLocation msgLocation) {
		int insertLocation = -1;
		for (int i=0; i<insertNode.getKeys().size(); i++) {
			if (insertNode.getKeys().get(i).getMessageId()>msgLocation.getMessageId()) {
				insertLocation = i;
				break;
			}
		}
		
		if (insertLocation==-1) {
			insertLocation = (insertNode.getKeys().size()==0)?0:(insertNode.getKeys().size());
		}
		insertNode.getKeys().add(insertLocation, msgLocation);
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
		
		return findInsertNode(searchNode.getChilds().get(searchNode.getChilds().size()-1), msgLocation);
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
				 sb.append(printNode(bTreeNode.getChilds().get(i)));
			 }
			 // 打印该节点
			 sb.append(bTreeNode.getKeys().get(i).getMessageId()+",");
		}
		// 在打印最后一个孩子节点
		if (CollectionUtils.isNotEmpty(bTreeNode.getChilds()) && bTreeNode.getChilds().size()>2) {
			sb.append(printNode(bTreeNode.getChilds().get(bTreeNode.getChilds().size()-1)));
		}
		
		return sb.toString();
	}
	
	/**
	 * 删除消息
	 * @param message
	 * @throws Exception
	 */
	public void delMessage(Message message) throws Exception {
		// 第一步找到root节点
		BTreeNode rootNode = bTreeMap.get(message.getDestination().getName());
		if (rootNode==null) {
			throw new Exception("该主题不存在");
		}
		// 第二步找到该结点位置
		BTreeNode exietNode = findBTreeNodeContainKey(rootNode, message.getMessageId());
		if (exietNode == null) {
			throw new Exception("该消息不存在");
		}
		
		BTreeNode newRootNode = null;
		// 找出该节点该关键字对应的左子树的最大关键字，替换该关键字。
		if (CollectionUtils.isNotEmpty(exietNode.getChilds())) {
			newRootNode = delKeyInInsideNodeAndAdjust(exietNode, message.getMessageId());
		} else {
			newRootNode = delKeyInLeafNodeAndAdjust(exietNode, message.getMessageId());
		}
		
		if (newRootNode!=null) {
			rootNode = newRootNode;
		}
	}

	private BTreeNode delKeyInInsideNodeAndAdjust(BTreeNode exietNode, long messageId) throws Exception {
		BTreeNode leftLeafNode = exietNode.getChilds().get(exietNode.getChilds().size()-1);
		while (CollectionUtils.isNotEmpty(leftLeafNode.getChilds())) {
			leftLeafNode = exietNode.getChilds().get(leftLeafNode.getChilds().size()-1);
		}
		for (int i=0; i<exietNode.getKeys().size(); i++) {
			MsgLocation msgLocation = exietNode.getKeys().get(i);
			if (msgLocation.getMessageId() == messageId) {
				// 替换
				msgLocation = leftLeafNode.getKeys().get(leftLeafNode.getKeys().size()-1);
			}
		}
		leftLeafNode.getKeys().remove(leftLeafNode.getKeys().size()-1);
		 return adjustInsideBTree(leftLeafNode);
	}

	/**
	 * 查找父节点中指向该节点的指针数组的下标
	 * @param searchNode
	 * @param msgLocation
	 * @return
	 * @throws Exception
	 */
	private int findParentKey(BTreeNode parentNode, BTreeNode childNode) throws Exception {
		// 遍历查找
		for(int i=0; i<parentNode.getChilds().size(); i++) {
			if (parentNode.getChilds().get(i).getKeys().size() == childNode.getKeys().size()) {
				for (int j=0; i<parentNode.getChilds().get(i).getKeys().size(); j++) {
					if (parentNode.getChilds().get(i).getKeys().get(j) != childNode.getKeys().get(j)) {
						break;
					}
				}
				return i;
			}
		}
		
		throw new Exception("未从父节点中找到指向该节点的指针");
	}
	
	
	/**
	 * 在叶节点中删除该节点
	 * @param exietNode
	 * @param message
	 * @throws Exception
	 */
	private BTreeNode delKeyInLeafNodeAndAdjust(BTreeNode exietNode, long messageId) throws Exception {
		
		// 1. 如果该结点，是root节点
		if (exietNode.getParent()==null) {
			// 在该节点中删除该关键字
			deleteKeyInBTreeNode(exietNode, messageId);
			return null;
		}
		
		// 2. 找到该节点
		int loc = -1;
		for (int i=0; i<exietNode.getKeys().size(); i++) {
			if (exietNode.getKeys().get(i).getMessageId() == messageId) {
				loc = i;
				break;
			}
		}
		
		if (loc<0) {
			throw new Exception("该结点不存在");
		}
		
		deleteKeyInBTreeNode(exietNode, messageId);
		
		return adjustInsideBTree(exietNode);
	}
	
	/**
	 * 该节点是否平衡,如果不平衡将其调整平衡
	 * @param parent
	 */
	private BTreeNode adjustInsideBTree(BTreeNode exietNode) throws Exception {
		if (exietNode.getParent()==null) {
			return null;
		}
		
		if (exietNode.getKeys().size() >= (Math.ceil(m/2.0)-1)) {
			return null;
		}
		
		// 查看兄弟结点是否富有，如果富有则借一个。
		// 先看左边
		// 查找指向该指针的位置
		int i = findParentKey(exietNode.getParent(), exietNode);
		if (i>0 && (exietNode.getParent().getChilds().get(i-1).getKeys().size()>=Math.ceil(m/2.0))) {
			BTreeNode leftNode = exietNode.getParent().getChilds().get(i-1);
			MsgLocation msgLocation = deleteKeyInBTreeNode(exietNode.getParent(), i-1);  // 删掉父节点中的该关键字
			int leftNodeKeySize = leftNode.getKeys().size();
			exietNode.getParent().getKeys().add(i-1, leftNode.getKeys().get(leftNodeKeySize-1));    // 在父节点中插入左孩子的最大关键字
			leftNode.getKeys().remove(leftNodeKeySize-1); // 在左孩子中删除最大关键字
			if (CollectionUtils.isNotEmpty(leftNode.getChilds())) {
				exietNode.getChilds().add(0,leftNode.getChilds().get(leftNode.getChilds().size()-1));
				leftNode.getChilds().remove(leftNode.getChilds().size()-1);
			}
			// 在该节点中插入
			exietNode.getKeys().add(0,msgLocation);
		}
		// 在看右边
		if (i<(exietNode.getParent().getKeys().size()-1) && (exietNode.getParent().getChilds().get(i+1).getKeys().size()>=Math.ceil(m/2.0))) {
			BTreeNode rightNode = exietNode.getParent().getChilds().get(i+1);
			MsgLocation msgLocation = deleteKeyInBTreeNode(exietNode.getParent(), i);  // 删掉父节点中的该关键字
			exietNode.getParent().getKeys().add(i, rightNode.getKeys().get(0));    // 在父节点中插入右孩子的最大关键字
			rightNode.getKeys().remove(0); // 在右孩子中删除最大关键字
			if (CollectionUtils.isNotEmpty(rightNode.getChilds())) {
				exietNode.getChilds().add(rightNode.getChilds().get(0));
				rightNode.getChilds().remove(0);
			}
			// 在该节点中插入
			exietNode.getKeys().add(msgLocation);
		}
		// 需要合并节点
		// 跟左兄弟合并
		if (i>0) {
			BTreeNode leftNode = exietNode.getParent().getChilds().get(i-1);
			leftNode.getKeys().add(exietNode.getParent().getKeys().get(i-1));
			for (MsgLocation msgLocation : exietNode.getKeys()) {
				leftNode.getKeys().add(msgLocation);
			}
			for (BTreeNode bTreeNode : exietNode.getChilds()) {
				leftNode.getChilds().add(bTreeNode);
			}
			// 在父节点中删除该节点
			exietNode.getParent().getKeys().remove(i-1);
			exietNode.getParent().getChilds().remove(i);
			// 判断父结点是否满足定义
			if (CollectionUtils.isNotEmpty(exietNode.getParent().getKeys())) {
				adjustInsideBTree(exietNode.getParent());
				return null;
			} else {
				exietNode.setParent(null);
				return exietNode;
			}
		} else {
			BTreeNode rightNode = exietNode.getParent().getChilds().get(i+1);
			rightNode.getKeys().add(exietNode.getParent().getKeys().get(i));
			for (int temp = exietNode.getKeys().size()-1 ; temp>=0; temp--) {
				rightNode.getKeys().add(0, exietNode.getKeys().get(temp));
			}
			for (int tempChild = exietNode.getChilds().size()-1 ; tempChild>=0; tempChild--) {
				rightNode.getChilds().add(0, exietNode.getChilds().get(tempChild));
			}
			// 在父节点中删除该节点
			exietNode.getParent().getKeys().remove(i);
			exietNode.getParent().getChilds().remove(i);
			// 判断父结点是否满足定义
			if (CollectionUtils.isNotEmpty(exietNode.getParent().getKeys())) {
				adjustInsideBTree(exietNode.getParent());
				return null;
			} else {
				exietNode.setParent(null);
				return exietNode;
			}
		}
		
	}

	/**
	 * 删除节点该下标的数，并且返回该节点
	 * @param exietNode
	 * @param i
	 */
	private MsgLocation deleteKeyInBTreeNode(BTreeNode exietNode, int i) {
		MsgLocation msgLocation = exietNode.getKeys().get(i);
		exietNode.getKeys().remove(i);
		return msgLocation;
	}

	private void deleteKeyInBTreeNode(BTreeNode node, long messageId) {
		if (CollectionUtils.isEmpty(node.getKeys())) {
			return;
		}
		for (int i=0; i<node.getKeys().size(); i++) {
			if (node.getKeys().get(i).getMessageId() == messageId) {
				node.getKeys().remove(i);
				return;
			}
		}
		
	}

	/**
	 * 查找该消息存在的位置
	 * @param rootNode
	 * @param message
	 * @return
	 */
	private BTreeNode findBTreeNodeContainKey(BTreeNode node, long messageId) {
		if (CollectionUtils.isEmpty(node.getKeys())) {
			return null;
		}
		for (int i=0; i<node.getKeys().size(); i++) {
			if (node.getKeys().get(i).getMessageId() == messageId) {
				return node;
			}
			if (node.getKeys().get(i).getMessageId() > messageId) {
				if (CollectionUtils.isNotEmpty(node.getChilds()) && node.getChilds().size()>=(i+1)) {
					return findBTreeNodeContainKey(node.getChilds().get(i), messageId);
				} else return null;
			}
		}
		if (CollectionUtils.isNotEmpty(node.getChilds()) && node.getChilds().size()>=(node.getKeys().size()+1)) {
			return findBTreeNodeContainKey(node.getChilds().get(node.getKeys().size()), messageId);
		} else return null;
	}

	public static void main(String[] args) {
		
		IndexCache indexCache = new IndexCache();
		List<Integer> list = new ArrayList<Integer>();
		Map<String, String> map = new HashMap<String, String>();
		for (int i =0;i<100;i++) {
			int a = new Random().nextInt(1000);
			if (StringUtils.isBlank(map.get(a+"")) ) {
				map.put(a+"", "true");
			} else {
				continue;
			}
			Message message = new Message();
			
			list.add(a);
			message.setMessageId(a);
			Destination destination = new Destination();
			destination.setName("a");
			message.setDestination(destination );
			MsgLocation msgLocation = new MsgLocation();
			msgLocation.setMessageId(a);
			try {
				indexCache.addMessageIndex(msgLocation, message);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		System.out.println("最终字符串为：" + indexCache.printNode("a"));

		System.out.println("list:::" + JSON.toJSONString(list));
		Collections.shuffle(list);
		System.out.println("list:::" + JSON.toJSONString(list));
		
		for (int i=0; i<list.size()-1; i++) {
			int temp = list.get(i);
			Message message = new Message();
			message.setMessageId(temp);
			Destination destination = new Destination();
			destination.setName("a");
			message.setDestination(destination );
			MsgLocation msgLocation = new MsgLocation();
			msgLocation.setMessageId(temp);
			try {
				indexCache.delMessage(message);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		System.out.println("最终字符串为：" + indexCache.printNode("a"));
	}
}
