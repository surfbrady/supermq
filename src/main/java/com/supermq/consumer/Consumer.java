package com.supermq.consumer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;

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
	
    private void registerAndConnect(SocketChannel sock, InetSocketAddress inetSocketAddress) throws ClosedChannelException {
		sockKey = sock.register(selector, SelectionKey.OP_CONNECT);
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
}
