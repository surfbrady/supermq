package com.supermq.consumer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.supermq.command.Message;

public class consumer {
	private String hostAddress;
	private int port;
    private SelectionKey sockKey;
    private SocketChannel sock;
    private List<Message> messagequeue = new ArrayList<Message>();
    private MessageListener listener;
    
    private final Selector selector = Selector.open();

	public consumer(String hostAddress, int port) throws IOException {
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
        new PullMessageThread().start();
        System.out.println("启动结束::::");
	}
	
	public class PullMessageThread extends Thread {
		public void run() {
			while (true) {
				try {
					selector.select();
				} catch (IOException e) {
					e.printStackTrace();
				}
				Iterator<SelectionKey> ite = selector.selectedKeys().iterator();
				while (ite.hasNext()) {
					SelectionKey key = ite.next();
					if (key.isReadable()) {
						
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

	public void pullMessage() {
		
	}
	
	public void consumerMessage(List<Message> messages) throws IOException {
		listener.onMessage(messages);
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
		
	}

}
