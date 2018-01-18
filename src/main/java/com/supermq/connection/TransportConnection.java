package com.supermq.connection;

import java.io.IOException;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

import com.alibaba.fastjson.JSON;
import com.supermq.boker.Topic;
import com.supermq.command.Command;
import com.supermq.command.Message;

public class TransportConnection {
	private SocketChannel socketChannel;
	private SelectionKey selectionKey;
	ByteBuffer lenBuffer = ByteBuffer.allocate(4);
    ByteBuffer incomingBuffer = lenBuffer;
    
	public TransportConnection(SocketChannel sc, SelectionKey sk) throws SocketException {
		this.selectionKey = sk;
		this.socketChannel = sc;
		socketChannel.socket().setTcpNoDelay(true);
		socketChannel.socket().setSoLinger(false, -1);
        sk.interestOps(SelectionKey.OP_READ);
	}

	public void doIO(SelectionKey k) {
		if (k.isReadable()) {
			try {
				if (incomingBuffer==lenBuffer) {
					int num = socketChannel.read(incomingBuffer);
					System.out.println("读取字节为：：：：；" + num);
					if (num<0) {
						throw new RuntimeException("读取失败");
					}
				}
				if (incomingBuffer.remaining()==0) {
					incomingBuffer.flip();
					int packetNum = incomingBuffer.getInt();
					System.out.println("读取字节packetNum为：：：：；" + packetNum);
					incomingBuffer = ByteBuffer.allocate(packetNum);
					readPacket();
				}
				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				close();
			}
		}
		
	}

	private void close() {
		closeSock();
		
	}

	private void closeSock() {
		System.out.println("关闭连接");
		try {
			socketChannel.socket().close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			socketChannel.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if (selectionKey!=null) {
			selectionKey.cancel();
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
		incomingBuffer.flip();
		System.out.println("处理接收到的消息:::" + incomingBuffer.remaining());
		// 解析消息
		byte[] bytes = new byte[incomingBuffer.remaining()];
		incomingBuffer.get(bytes, 0, bytes.length);
		Command command = JSON.parseObject(bytes, Message.class);
		if (command.isMessage()) {
			System.out.println(" message it is ");
			Message message = (Message)command;
			message.setMessageId(createMessageId());
			try {
				Topic.getInstance().addMessage(message);
			} catch (Exception e) {
				e.printStackTrace();
			}
			System.out.println("message context:" + message.getContext());
		}
	}

	private long createMessageId() {
		return System.currentTimeMillis();
	}

}
