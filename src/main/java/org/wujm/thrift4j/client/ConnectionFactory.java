package org.wujm.thrift4j.client;

import lombok.RequiredArgsConstructor;
import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.thrift.transport.TTransport;
import org.wujm.thrift4j.client.transport.TransportConfig;

/**
 * @author wujunmin
 */
@RequiredArgsConstructor
public class ConnectionFactory extends BasePooledObjectFactory<TTransport> {
    private final TransportConfig config;

    @Override
    public TTransport create() throws Exception {
        return config.generateTransport();
    }

    @Override
    public PooledObject<TTransport> wrap(TTransport obj) {
        return new DefaultPooledObject<>(obj);
    }

    @Override
    public void destroyObject(PooledObject<TTransport> p) {
        p.getObject().close();
    }

    @Override
    public boolean validateObject(PooledObject<TTransport> p) {
        return p.getObject().isOpen();
    }

    @Override
    public void activateObject(PooledObject<TTransport> p) throws Exception {
        TTransport transport = p.getObject();
        if (!transport.isOpen()) {
            transport.open();
        }
        super.activateObject(p);
    }

    @Override
    public void passivateObject(PooledObject<TTransport> p) throws Exception {
        super.passivateObject(p);
    }
}
