package org.wujm.thrift4j.client;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TServiceClient;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.wujm.thrift4j.client.transport.TransportConfig;
import org.wujm.thrift4j.exception.ThriftException;

import java.io.Closeable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.function.Function;

/**
 * @author wujunmin
 */
@Slf4j
public class ClientPool<T extends TServiceClient> implements Closeable {
    private final ClientPoolConfig poolConfig;

    private final ObjectPool<TTransport> pool;
    private final Class<T> serviceClientClazz;
    private final Function<TTransport, T> clientFactory;

    public ClientPool(ClientPoolConfig poolConfig, TransportConfig transportConfig, Class<T> serviceClientClazz, Function<TTransport, T> clientFactory) {
        this.poolConfig = poolConfig;
        this.pool = new GenericObjectPool<>(new ConnectionFactory(transportConfig));
        this.serviceClientClazz = serviceClientClazz;
        this.clientFactory = clientFactory;
    }

    public <X> X getClient() {
        //noinspection unchecked
        return (X) Proxy.newProxyInstance(serviceClientClazz.getClassLoader(), serviceClientClazz.getInterfaces(), (proxy, method, args) -> {
            int retryCount = 0;
            while (true) {
                TTransport transport = null;
                try {
                    try {
                        transport = pool.borrowObject();
                        log.debug("borrow {}", transport);
                        T client = clientFactory.apply(transport);
                        return method.invoke(client, args);
                    } catch (InvocationTargetException e) {
                        Throwable targetExc = e.getTargetException();
                        if (targetExc instanceof TApplicationException) {
                            log.info("Backend throws exception", targetExc);
                            throw targetExc;
                        }
                        if (targetExc instanceof TTransportException) {
                            pool.invalidateObject(transport);
                            transport = null;
                        }
                        log.warn("Invocation failed", e);
                        throw new ThriftException("invoke fail", e.getTargetException());
                    }
                } catch (Exception e) {
                    if (e instanceof TApplicationException) {
                        throw e;
                    }
                    if (retryCount < poolConfig.getRetry()) {
                        retryCount++;
                    } else {
                        log.warn("Invocation failed after retried " + poolConfig.getRetry() + " times", e);
                        if (e instanceof ThriftException) {
                            throw e.getCause();
                        } else {
                            throw new ThriftException("Getting client from pool failed.", e);
                        }
                    }
                } finally {
                    if (transport != null) {
                        pool.returnObject(transport);
                    }
                }
            }
        });
    }

    @Override
    public void close() {
        pool.close();
    }
}
