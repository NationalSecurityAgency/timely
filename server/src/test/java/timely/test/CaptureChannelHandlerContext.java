package timely.test;

import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelProgressivePromise;
import io.netty.channel.ChannelPromise;
import io.netty.channel.DefaultChannelPromise;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;
import io.netty.util.concurrent.EventExecutor;

import java.net.SocketAddress;

public class CaptureChannelHandlerContext implements ChannelHandlerContext {

    public static class TestDefaultPromise extends DefaultChannelPromise {

        public TestDefaultPromise(Channel channel) {
            super(channel);
        }

        @Override
        public boolean isSuccess() {
            return true;
        }
    }

    public Object msg = null;
    private TestDefaultPromise promise = new TestDefaultPromise(null);

    @Override
    public <T> Attribute<T> attr(AttributeKey<T> key) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Channel channel() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public EventExecutor executor() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String name() {
        return this.getClass().getSimpleName();
    }

    @Override
    public ChannelHandler handler() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean isRemoved() {
        return false;
    }

    @Override
    public ChannelHandlerContext fireChannelRegistered() {
        return this;
    }

    @Override
    public ChannelHandlerContext fireChannelUnregistered() {
        return this;
    }

    @Override
    public ChannelHandlerContext fireChannelActive() {
        return this;
    }

    @Override
    public ChannelHandlerContext fireChannelInactive() {
        return this;
    }

    @Override
    public ChannelHandlerContext fireExceptionCaught(Throwable cause) {
        return this;
    }

    @Override
    public ChannelHandlerContext fireUserEventTriggered(Object event) {
        return this;
    }

    @Override
    public ChannelHandlerContext fireChannelRead(Object msg) {
        return this;
    }

    @Override
    public ChannelHandlerContext fireChannelReadComplete() {
        return this;
    }

    @Override
    public ChannelHandlerContext fireChannelWritabilityChanged() {
        return this;
    }

    @Override
    public ChannelFuture bind(SocketAddress localAddress) {
        return promise;
    }

    @Override
    public ChannelFuture connect(SocketAddress remoteAddress) {
        return promise;
    }

    @Override
    public ChannelFuture connect(SocketAddress remoteAddress, SocketAddress localAddress) {
        return promise;
    }

    @Override
    public ChannelFuture disconnect() {
        return promise;
    }

    @Override
    public ChannelFuture close() {
        return promise;
    }

    @Override
    public ChannelFuture deregister() {
        return promise;
    }

    @Override
    public ChannelFuture bind(SocketAddress localAddress, ChannelPromise promise) {
        return promise;
    }

    @Override
    public ChannelFuture connect(SocketAddress remoteAddress, ChannelPromise promise) {
        return promise;
    }

    @Override
    public ChannelFuture connect(SocketAddress remoteAddress, SocketAddress localAddress, ChannelPromise promise) {
        return promise;
    }

    @Override
    public ChannelFuture disconnect(ChannelPromise promise) {
        return promise;
    }

    @Override
    public ChannelFuture close(ChannelPromise promise) {
        return promise;
    }

    @Override
    public ChannelFuture deregister(ChannelPromise promise) {
        return promise;
    }

    @Override
    public ChannelHandlerContext read() {
        return this;
    }

    @Override
    public ChannelFuture write(Object msg) {
        return promise;
    }

    @Override
    public ChannelFuture write(Object msg, ChannelPromise promise) {
        return promise;
    }

    @Override
    public ChannelHandlerContext flush() {
        return this;
    }

    @Override
    public ChannelFuture writeAndFlush(Object msg, ChannelPromise promise) {
        return promise;
    }

    @Override
    public ChannelFuture writeAndFlush(Object msg) {
        this.msg = msg;
        return promise;
    }

    @Override
    public ChannelPipeline pipeline() {
        return null;
    }

    @Override
    public ByteBufAllocator alloc() {
        return null;
    }

    @Override
    public ChannelPromise newPromise() {
        return promise;
    }

    @Override
    public ChannelProgressivePromise newProgressivePromise() {
        return null;
    }

    @Override
    public ChannelFuture newSucceededFuture() {
        return promise;
    }

    @Override
    public ChannelFuture newFailedFuture(Throwable cause) {
        return promise;
    }

    @Override
    public ChannelPromise voidPromise() {
        return promise;
    }

}
