package com.supermq.connection.netty;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.handler.codec.string.StringDecoder;
import org.jboss.netty.handler.codec.string.StringEncoder;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

/**
 * 客户端连接处理类
 * @author brady
 *
 */
public class TransportConnector {

	public TransportConnector () {
		start();
	}
	
	public void start() {
		ChannelFactory factory = new NioServerSocketChannelFactory(
				Executors.newCachedThreadPool(),//boss线程池
				Executors.newCachedThreadPool(),//worker线程池
				8//worker线程数
				);
		ServerBootstrap bootstrap = new ServerBootstrap(factory);
		/**
		 * 对于每一个连接channel, server都会调用PipelineFactory为该连接创建一个ChannelPipline
		 */
		bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
			public ChannelPipeline getPipeline() throws Exception {
				ChannelPipeline pipeline = Channels.pipeline();
				pipeline.addLast("decoder", new StringDecoder());
				pipeline.addLast("encoder", new StringEncoder());
				pipeline.addLast("handler", new SocketHandler());
				return pipeline;
			}
		});

		Channel channel = bootstrap.bind(new InetSocketAddress("127.0.0.1", 8080));
		System.out.println("server start success!");
	}
	
	
}
