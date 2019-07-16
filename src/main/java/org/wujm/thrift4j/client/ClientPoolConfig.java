package org.wujm.thrift4j.client;

import lombok.Data;

/**
 * @author wujunmin
 */
@Data
public class ClientPoolConfig {
    private int retry;

    public ClientPoolConfig(int retry) {
        this.retry = retry;
    }
}
