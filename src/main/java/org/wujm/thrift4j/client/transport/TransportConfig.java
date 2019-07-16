package org.wujm.thrift4j.client.transport;

import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

/**
 * @author wujunmin
 */
public interface TransportConfig {
    /**
     * 生成 TTransport
     *
     * @return TTransport
     * @throws TTransportException Transport exceptions
     */
    TTransport generateTransport() throws TTransportException;
}
