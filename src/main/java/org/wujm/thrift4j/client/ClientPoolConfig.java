package org.wujm.thrift4j.client;

import lombok.Getter;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.thrift.transport.TTransport;

/**
 * @author wujunmin
 */
public class ClientPoolConfig extends GenericObjectPoolConfig<TTransport> {
    @Getter
    private int retry;

    public ClientPoolConfig(int retry) {
        this.retry = retry;
        this.setTestOnBorrow(true);
    }
}
