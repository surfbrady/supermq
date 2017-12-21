package com.supermq.connection;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Set;

/**
 * Created by brady on 16/8/5.
 */
public class ZkServer {
    public static void main(String[] args) {
        try {
            Selector selector=null;
            selector = Selector.open();
            ServerSocketChannel ss=null;
            ss = ServerSocketChannel.open();
            ss.socket().bind(new InetSocketAddress(9999));
            ss.configureBlocking(false);
            ss.register(selector, SelectionKey.OP_ACCEPT);
            while (true){
                selector.select();
                Set<SelectionKey> selected=selector.selectedKeys();
                for (SelectionKey k : selected) {
                    if ((k.readyOps() & SelectionKey.OP_ACCEPT) != 0) {
                        SocketChannel sc = ((ServerSocketChannel) k
                                .channel()).accept();
                        System.out.println("ip:"+sc.socket().getRemoteSocketAddress()+"port:"+sc.socket().getPort());
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
        }
    }
}
