package com.supermq.product;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.alibaba.fastjson.JSON;
import com.supermq.command.Destination;
import com.supermq.command.Message;

public class Productor {
	private String hostAddress;
	private int port;
    private SelectionKey sockKey;
    private SocketChannel sock;
    private List<Message> messagequeue = new ArrayList<Message>();
    
    private final Selector selector = Selector.open();

	public Productor (String hostAddress, int port) throws IOException {
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
        sock = createSock();
        registerAndConnect(sock, inetSocketAddress);
        new SendPacketThread().start();
        System.out.println("启动结束::::");
	}
	
	public class SendPacketThread extends Thread {
		public void run() {
			while (true) {
				if (messagequeue.size()>0) {
					Message msg = messagequeue.remove(0);
					int num = JSON.toJSONString(msg).getBytes().length;
					ByteBuffer bb = ByteBuffer.allocate(num+4);
					bb.putInt(num);
					System.out.println("写入字节num" + num);
					bb.put(JSON.toJSONString(msg).getBytes());
					if (bb.remaining()==0) {
						System.out.println("没有剩余空间！");
					}
					bb.flip();
					int a;
					try {
						a = sock.write(bb);
						System.out.println("写入字节" + a);
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}
		}
	}

	private void registerAndConnect(SocketChannel sock, InetSocketAddress inetSocketAddress) throws IOException {
		sockKey = sock.register(selector, SelectionKey.OP_CONNECT);
		sock.connect(inetSocketAddress);
		boolean isOver = false;
		while ( !isOver ) {
            selector.select();  
            Iterator<SelectionKey> ite = selector.selectedKeys().iterator();
            boolean init = false;
            while(ite.hasNext()){  
                SelectionKey key = (SelectionKey) ite.next();  
                ite.remove();  
                   
                if(key.isConnectable()) {
                	if(sock.isConnectionPending()) {
                        if(sock.finishConnect()) {
                    		System.out.println("连接上服务器！");
                        	sock.register(selector, SelectionKey.OP_READ);
                        	init = true;
                        }
                	}
                }
            }
            
            if (init) {
            	break;
            }
		}
	}

	public void sendMessage(String context, String topicName) throws IOException {
		Message message = new Message();
		message.setContext(context);
		Destination destination = new Destination();
		destination.setName(topicName);
		message.setDestination(destination);
		messagequeue.add(message);
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
			Productor productor = new Productor("127.0.0.1", 9999);
			for (int i=0; i<10; i++) {
				String message = "我的第很多条消息哟" + i;
				productor.sendMessage(message, "a");
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
