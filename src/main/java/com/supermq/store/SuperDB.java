package com.supermq.store;

import com.supermq.command.Message;

public class SuperDB {
	private Journal journal;
	private IndexCache indexCache;
	
	public SuperDB () {
		journal = new Journal();
		indexCache = new IndexCache();
	}
	
	public void addMessage(Message msg) throws Exception {
		MsgLocation msgLocation = journal.addMessage(msg);
		
	}
	
	public void delMessage(Message msg) throws Exception {
		MsgLocation msgLocation = journal.addMessage(msg);
		
	}
}
