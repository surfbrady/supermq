package com.supermq.consumer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;

import com.alibaba.fastjson.JSON;
import com.supermq.command.Message;

public class Consumer {
	private String hostAddress;
	private int port;
    private SelectionKey sockKey;
    private final Selector selector = Selector.open();

	public Consumer (String hostAddress, int port) throws IOException {
		this.hostAddress = hostAddress;
		this.port = port;
		InetSocketAddress inetSocketAddress = new InetSocketAddress(hostAddress, port);
		try {
			connect(inetSocketAddress);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private void connect(InetSocketAddress inetSocketAddress) throws IOException {
        SocketChannel sock = createSock();
        registerAndConnect(sock, inetSocketAddress);
	}
	
    private void registerAndConnect(SocketChannel sock, InetSocketAddress inetSocketAddress) throws IOException {
		sockKey = sock.register(selector, SelectionKey.OP_CONNECT);
		boolean connectFlag = sock.connect(inetSocketAddress);
		if (connectFlag) {
			System.out.println("连接上服务器！");
			Message message = new Message();
			message.setContext("第一条消息");
			int num = JSON.toJSONString(message).getBytes().length;
			ByteBuffer bb = ByteBuffer.allocate(num+4);
			bb.putInt(num);
			bb.put(JSON.toJSONString(message).getBytes());
			if (bb.remaining()==0) {
				System.out.println("没有剩余空间！");
			}
			sock.write(bb);
		}
	}

	/**
     * create a socket channel.
     * @return the created socket channel
     * @throws IOException
     */
    SocketChannel createSock() throws IOException {
        SocketChannel sock;
        sock = SocketChannel.open();
        sock.configureBlocking(false);
        sock.socket().setSoLinger(false, -1);
        sock.socket().setTcpNoDelay(true);
        return sock;
    }
    
	public static void main(String[] args) {
		try {
			new Consumer("127.0.0.1", 9999);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
