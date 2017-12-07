package com.supermq.client;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;

/**
 * Created by brady on 16/8/5.
 */
public class ZkClient {
    public static void main(String[] args) {
        try {
            SocketChannel socketChannel = SocketChannel.open();
            socketChannel.connect(new InetSocketAddress("127.0.0.1", 9999));
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
        }
    }
}
