package org.wujm.thrift4j.client;

import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TServiceClient;
import org.apache.thrift.protocol.TProtocolException;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.wujm.thrift4j.client.transport.TransportConfig;
import org.wujm.thrift4j.exception.Thrift4jException;

import java.io.Closeable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.NoSuchElementException;
import java.util.function.Function;

/**
 * @author wujunmin
 */
@Slf4j
public class ClientPool<T extends TServiceClient> implements Closeable {
    private final ClientPoolConfig poolConfig;

    private final ObjectPool<TTransport> transportPool;
    private final Class<T> serviceClientClazz;
    private final Function<TTransport, T> clientFactory;

    @Builder
    public ClientPool(
            ClientPoolConfig poolConfig, TransportConfig transportConfig, Class<T> serviceClientClazz,
            Function<TTransport, T> clientFactory, Function<T, Boolean> validator
    ) {
        this.poolConfig = poolConfig;
        this.transportPool = new GenericObjectPool<>(
                ConnectionFactory.builder()
                        .config(transportConfig)
                        .validator(t -> validator.apply(clientFactory.apply(t)))
                        .build(),
                this.poolConfig
        );
        this.serviceClientClazz = serviceClientClazz;
        this.clientFactory = clientFactory;
    }

    public <X> X getClient() {
        //noinspection unchecked
        return (X) Proxy.newProxyInstance(
                serviceClientClazz.getClassLoader(), serviceClientClazz.getInterfaces(), (proxy, method, args) -> {
                    int retryCount = 0;
                    int maxRetry = poolConfig.getRetry();
                    while (true) {
                        TTransport transport = null;
                        try {
                            try {
                                /* Throws NoSuchElementException */
                                transport = transportPool.borrowObject();
                                log.debug("Borrow transport {}", transport);

                                T client = clientFactory.apply(transport);

                                /* Throws InvocationTargetException */
                                return method.invoke(client, args);

                            } catch (InvocationTargetException e) {
                                Throwable targetExc = e.getTargetException();

                                /* Transport failure */
                                if (targetExc instanceof TTransportException) {
                                    log.debug("Invalidate transport " + transport, e);
                                    transportPool.invalidateObject(transport);
                                    transport = null;
                                }
                                throw e;
                            }
                        } catch (Exception e) {
                            /* Pointless to retry, just throw */
                            if (e instanceof TApplicationException || e instanceof TProtocolException) {
                                throw e;
                            }

                            /* Retryable */
                            log.warn("Invocation failed.", e);
                            if (retryCount < maxRetry) {
                                retryCount++;
                            } else {
                                /* BorrowObject failed, throw Thrift4jException */
                                if (e instanceof NoSuchElementException) {
                                    throw new Thrift4jException(
                                            "Getting transport from pool failed after " +
                                                    retryCount + " retries.", e);
                                }
                                /* Invocation failed, throw targetExc */
                                else {
                                    log.warn("Invocation failed after " + maxRetry + " retries", e);
                                    throw e;
                                }
                            }
                        } finally {
                            if (transport != null) {
                                transportPool.returnObject(transport);
                            }
                        }
                    }
                });
    }

    @Override
    public void close() {
        transportPool.close();
    }
}
