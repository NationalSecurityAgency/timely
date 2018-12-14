package timely.balancer.configuration;

import javax.validation.Valid;

import org.apache.commons.pool2.impl.GenericKeyedObjectPoolConfig;
import org.springframework.boot.context.properties.NestedConfigurationProperty;
import timely.configuration.Server;

public class BalancerServer extends Server {

    @Valid
    @NestedConfigurationProperty
    private GenericKeyedObjectPoolConfig udpClientPool = new GenericKeyedObjectPoolConfig();

    @Valid
    @NestedConfigurationProperty
    private GenericKeyedObjectPoolConfig tcpClientPool = new GenericKeyedObjectPoolConfig();

    private int numTcpPools = 1;

    private int tcpBufferSize = -1;

    public GenericKeyedObjectPoolConfig getUdpClientPool() {
        return udpClientPool;
    }

    public GenericKeyedObjectPoolConfig getTcpClientPool() {
        return tcpClientPool;
    }

    public int getNumTcpPools() {
        return numTcpPools;
    }

    public void setNumTcpPools(int numTcpPools) {
        this.numTcpPools = numTcpPools;
    }

    public int getTcpBufferSize() {
        return tcpBufferSize;
    }

    public void setTcpBufferSize(int tcpBufferSize) {
        this.tcpBufferSize = tcpBufferSize;
    }
}
