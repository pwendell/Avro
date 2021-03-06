/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.avro.ipc;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;

import org.apache.avro.ipc.NettyTransportCodec.NettyDataPack;
import org.apache.avro.ipc.NettyTransportCodec.NettyFrameDecoder;
import org.apache.avro.ipc.NettyTransportCodec.NettyFrameEncoder;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.ChannelGroupFuture;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Netty-based RPC {@link Server} implementation.
 */
public class NettyServer implements Server {
  private static final Logger LOG = LoggerFactory.getLogger(NettyServer.class
      .getName());

  private Responder responder;

  private Channel serverChannel;
  private ChannelGroup allChannels = new DefaultChannelGroup(
      "avro-netty-server");
  private ChannelFactory channelFactory;
  private CountDownLatch closed = new CountDownLatch(1);
  
  public NettyServer(Responder responder, InetSocketAddress addr) {
    this.responder = responder;
    channelFactory = new NioServerSocketChannelFactory(Executors
        .newCachedThreadPool(), Executors.newCachedThreadPool());
    ServerBootstrap bootstrap = new ServerBootstrap(channelFactory);
    bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
      @Override
      public ChannelPipeline getPipeline() throws Exception {
        ChannelPipeline p = Channels.pipeline();
        p.addLast("frameDecoder", new NettyFrameDecoder());
        p.addLast("frameEncoder", new NettyFrameEncoder());
        p.addLast("handler", new NettyServerAvroHandler());
        return p;
      }
    });
    serverChannel = bootstrap.bind(addr);
    allChannels.add(serverChannel);
  }

  @Override
  public void start() {
    // No-op.
  }
  
  @Override
  public void close() {
    ChannelGroupFuture future = allChannels.close();
    future.awaitUninterruptibly();
    channelFactory.releaseExternalResources();
    closed.countDown();
  }
  
  @Override
  public int getPort() {
    return ((InetSocketAddress) serverChannel.getLocalAddress()).getPort();
  }

  @Override
  public void join() throws InterruptedException {
    closed.await();
  }

  /**
   * Avro server handler for the Netty transport 
   */
  class NettyServerAvroHandler extends SimpleChannelUpstreamHandler {

    @Override
    public void handleUpstream(ChannelHandlerContext ctx, ChannelEvent e)
        throws Exception {
      if (e instanceof ChannelStateEvent) {
        LOG.info(e.toString());
      }
      super.handleUpstream(ctx, e);
    }

    @Override
    public void channelOpen(ChannelHandlerContext ctx, ChannelStateEvent e)
        throws Exception {
      allChannels.add(e.getChannel());
      super.channelOpen(ctx, e);
    }

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
      try {
        NettyDataPack dataPack = (NettyDataPack) e.getMessage();
        List<ByteBuffer> req = dataPack.getDatas();
        List<ByteBuffer> res = responder.respond(req);
        dataPack.setDatas(res);
        e.getChannel().write(dataPack);
      } catch (IOException ex) {
        LOG.warn("unexpect error");
      } finally {
        e.getChannel().close();
      }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
      LOG.warn("Unexpected exception from downstream.", e.getCause());
      e.getChannel().close();
    }

  }

}
