package com.supermq.connection;

import java.io.IOException;
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
		if (k.isReadable()) {
			try {
				if (incomingBuffer==lenBuffer) {
					int num = socketChannel.read(incomingBuffer);
					if (num<0) {
						throw new RuntimeException("读取失败");
					}
				}
				if (incomingBuffer.remaining()==0) {
					incomingBuffer = ByteBuffer.allocate(incomingBuffer.getInt());
					readPacket();
				}
				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
	}

	private void readPacket() {
		if (incomingBuffer.remaining()!=0) {
			int num;
			try {
				num = socketChannel.read(incomingBuffer);
				if (num<0) {
					throw new RuntimeException("读取失败");
				}
				
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		if (incomingBuffer.remaining()==0) {
			packetReceived();
			lenBuffer.clear();
			incomingBuffer = lenBuffer;
		}
	}

	/**
	 * 处理接收到的消息
	 */
	private void packetReceived() {
		// TODO Auto-generated method stub
		
	}

}
