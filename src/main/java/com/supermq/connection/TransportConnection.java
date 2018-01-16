package com.supermq.connection;

import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

public class TransportConnection {
	private SocketChannel socketChannel;
	private SelectionKey selectionKey;
	ByteBuffer lenBuffer = ByteBuffer.allocate(4);
    ByteBuffer incomingBuffer = lenBuffer;
    
	public TransportConnection(SocketChannel sc, SelectionKey sk) {
		this.selectionKey = sk;
		this.socketChannel = sc;
        sk.interestOps(SelectionKey.OP_READ);
	}

	public void doIO(SelectionKey k) {
		// TODO Auto-generated method stub
		
	}

}
