package org.wujm.thrift4j.client;

import lombok.Data;
import lombok.RequiredArgsConstructor;
import org.apache.commons.pool2.ObjectPool;
import org.apache.thrift.transport.TTransport;

import java.io.Closeable;

/**
 * @author wujunmin
 */
@Data
@RequiredArgsConstructor
public class TransportWrapper implements Closeable {
    private final TTransport transport;
    private final ObjectPool<TTransport> pool;

    @Override
    public void close() {
        try {
            pool.returnObject(transport);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
