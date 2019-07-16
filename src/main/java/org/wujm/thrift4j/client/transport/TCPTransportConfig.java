package org.wujm.thrift4j.client.transport;

import lombok.Data;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

/**
 * @author wujunmin
 */
@Data
public class TCPTransportConfig implements TransportConfig {
    private final String host;
    private final int port;
    private final int timeout;

    /**
     * @param host    host
     * @param port    port
     * @param timeout timeout in milliseconds
     */
    public TCPTransportConfig(String host, int port, int timeout) {
        this.host = host;
        this.port = port;
        this.timeout = timeout;
    }

    @Override
    public TTransport generateTransport() {
        return new TSocket(host, port, timeout);
    }
}
