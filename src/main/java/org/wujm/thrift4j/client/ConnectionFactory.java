package org.wujm.thrift4j.client;

import lombok.Builder;
import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.thrift.transport.TTransport;
import org.wujm.thrift4j.client.transport.TransportConfig;

import java.util.function.Function;

/**
 * @author wujunmin
 */
public class ConnectionFactory extends BasePooledObjectFactory<TTransport> {
    private final TransportConfig config;
    private final Function<TTransport, Boolean> validator;

    @Builder
    public ConnectionFactory(TransportConfig config, Function<TTransport, Boolean> validator) {
        this.config = config;
        this.validator = validator == null ? TTransport::isOpen : validator;
    }

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
        return validator.apply(p.getObject());
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
