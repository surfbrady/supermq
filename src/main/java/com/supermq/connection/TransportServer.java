package com.supermq.connection;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Set;

/**
 * 监听端口，监听客户端连接
 * @author brady
 *
 */
public class TransportServer implements Runnable {
	ServerSocketChannel ss;
    final Selector selector = Selector.open();
	Thread thread;
	
	public TransportServer () throws IOException {
		
	}
	
	public void config() {
		thread = new Thread(this);
		try {
            ss = ServerSocketChannel.open();
            ss.socket().bind(new InetSocketAddress(9999));
            ss.configureBlocking(false);
            ss.register(selector, SelectionKey.OP_ACCEPT);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
        }
	}
	
	public void start () {
		thread.start();
	}
	
	public void run() {
        while (!ss.socket().isClosed()) {
            try {
                selector.select(1000);
                Set<SelectionKey> selected;
                synchronized (this) {
                    selected = selector.selectedKeys();
                }
                ArrayList<SelectionKey> selectedList = new ArrayList<SelectionKey>(
                        selected);
                Collections.shuffle(selectedList);
                for (SelectionKey k : selectedList) {
                    if ((k.readyOps() & SelectionKey.OP_ACCEPT) != 0) {
                        SocketChannel sc = ((ServerSocketChannel) k
                                .channel()).accept();
                        InetAddress ia = sc.socket().getInetAddress();
                        sc.configureBlocking(false);
                        SelectionKey sk = sc.register(selector,SelectionKey.OP_READ);
                        NIOServerCnxn cnxn = createConnection(sc, sk);
                        sk.attach(cnxn);
                        addCnxn(cnxn);
                    } else if ((k.readyOps() & (SelectionKey.OP_READ | SelectionKey.OP_WRITE)) != 0) {
                        NIOServerCnxn c = (NIOServerCnxn) k.attachment();
                        c.doIO(k);
                    } else {
                    }
                }
                selected.clear();
            } catch (RuntimeException e) {
            	e.printStackTrace();
            } catch (Exception e) {
            	e.printStackTrace();
            }
        }
	}
	
}
