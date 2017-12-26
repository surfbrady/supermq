package com.supermq.entity;

/**
 * 消息目的地
 * 
 * 
 * @author brady
 *
 */
public class Destination {
	private String name;    // 目的地标示
	private int type; // 1 topic 2 queue
	
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public int getType() {
		return type;
	}
	public void setType(int type) {
		this.type = type;
	}
}
