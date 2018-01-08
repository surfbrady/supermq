package com.supermq.store;

import java.util.ArrayList;
import java.util.List;

/**
 * key         k0,k1,k2,k3,k4......
 * childs   p0,p1,p2,p3,p4,p5......
 * 关系          k2<p3<k3
 * B树节点
 * @author brady
 *
 */
public class BTreeNode {
	private BTreeNode parent;
	private List<MsgLocation> keys = new ArrayList<MsgLocation>();
	private List<BTreeNode> childs = new ArrayList<BTreeNode>();
	
	public BTreeNode getParent() {
		return parent;
	}
	public void setParent(BTreeNode parent) {
		this.parent = parent;
	}
	public List<BTreeNode> getChilds() {
		return childs;
	}
	public void setChilds(List<BTreeNode> childs) {
		this.childs = childs;
	}
	public List<MsgLocation> getKeys() {
		return keys;
	}
	public void setKeys(List<MsgLocation> keys) {
		this.keys = keys;
	}
	
}
