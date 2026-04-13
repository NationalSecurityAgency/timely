package timely.balancer.configuration;

import javax.validation.Valid;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.NestedConfigurationProperty;
import org.springframework.cloud.context.config.annotation.RefreshScope;

import timely.client.tcp.TcpClient;
import timely.client.udp.UdpClient;
import timely.common.configuration.ServerProperties;

@RefreshScope
@ConfigurationProperties(prefix = "timely.balancer.server")
public class BalancerServerProperties extends ServerProperties {

    @Valid
    @NestedConfigurationProperty
    private GenericKeyedObjectPoolConfiguration<UdpClient> udpClientPool = new GenericKeyedObjectPoolConfiguration<>();

    @Valid
    @NestedConfigurationProperty
    private GenericKeyedObjectPoolConfiguration<TcpClient> tcpClientPool = new GenericKeyedObjectPoolConfiguration<>();

    private int numTcpPools = 1;

    private int tcpBufferSize = -1;

    public GenericKeyedObjectPoolConfiguration<UdpClient> getUdpClientPool() {
        return udpClientPool;
    }

    public GenericKeyedObjectPoolConfiguration<TcpClient> getTcpClientPool() {
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
