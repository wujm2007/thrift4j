package org.wujm.thrift4j.client;

import lombok.extern.slf4j.Slf4j;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransportException;
import org.junit.Test;
import org.wujm.thrift4j.client.service.TestThriftService;
import org.wujm.thrift4j.client.service.TestThriftServiceHandler;
import org.wujm.thrift4j.client.transport.TCPTransportConfig;
import org.wujm.thrift4j.client.transport.TransportConfig;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class TestThriftClientPool {

    private static final int PORT = 9090;
    private static volatile TSimpleServer server;
    private static volatile TServerTransport serverTransport;

    private static TSimpleServer getServer(TestThriftService.Iface iface) {
        try {
            if (serverTransport != null) {
                serverTransport.close();
            }
            serverTransport = new TServerSocket(PORT);
        } catch (TTransportException e) {
            e.printStackTrace();
        }
        TServer.Args processor = new TSimpleServer.Args(serverTransport)
                .inputTransportFactory(new TFramedTransport.Factory())
                .outputTransportFactory(new TFramedTransport.Factory())
                .processor(new TestThriftService.Processor<>(iface));
        server = new TSimpleServer(processor);
        return server;
    }

    @Test
    public void testEcho() throws InterruptedException {
        if (server != null) {
            server.stop();
        }
        new Thread(() -> getServer(new TestThriftServiceHandler.Handler0()).serve()).start();

        TransportConfig transportConfig = new TCPTransportConfig("localhost", PORT, 200);
        ClientPoolConfig poolConfig = new ClientPoolConfig(3);
        ClientPool<TestThriftService.Client> pool = new ClientPool<>(
                poolConfig,
                transportConfig,
                TestThriftService.Client.class,
                transport -> new TestThriftService.Client(new TBinaryProtocol(new TFramedTransport(transport)))
        );

        ExecutorService executorService = Executors.newFixedThreadPool(10);
        AtomicInteger successCnt = new AtomicInteger(), failCnt = new AtomicInteger();

        int times = 100;

        for (int i = 0; i < times; i++) {
            int counter = i;
            Thread.sleep(20);
            executorService.submit(() -> {
                try {
                    TestThriftService.Iface client = pool.getClient();
                    String request = "Hello " + counter + "!";
                    String response = client.echo(request);
                    if (request.equals(response)) {
                        successCnt.addAndGet(1);
                    }
                } catch (Throwable e) {
                    failCnt.addAndGet(1);
                    log.error("Get client fail", e);
                }
            });
        }

        executorService.shutdown();
        executorService.awaitTermination(1, TimeUnit.MINUTES);

        assert successCnt.intValue() == times;
        assert failCnt.intValue() == 0;

    }

    @Test
    public void testServerReset() throws InterruptedException {
        if (server != null) {
            server.stop();
        }

        new Thread(() -> getServer(new TestThriftServiceHandler.Handler1()).serve()).start();

        TransportConfig transportConfig = new TCPTransportConfig("localhost", PORT, 1000);
        ClientPoolConfig poolConfig = new ClientPoolConfig(3);
        ClientPool<TestThriftService.Client> pool = new ClientPool<>(
                poolConfig,
                transportConfig,
                TestThriftService.Client.class,
                transport -> new TestThriftService.Client(new TBinaryProtocol(new TFramedTransport(transport)))
        );

        ExecutorService executorService = Executors.newFixedThreadPool(10);
        AtomicInteger successCnt = new AtomicInteger(), failCnt = new AtomicInteger();

        int times = 10;

        for (int i = 0; i < times; i++) {
            int counter = i;
            executorService.submit(() -> {
                try {
                    Thread.sleep((counter > 5 ? counter + 50 : counter) * 100);
                    TestThriftService.Iface client = pool.getClient();
                    String request = "Hello " + counter + "!";
                    String response = client.echo(request);
                    log.info("Invoke success, {}", response);
                    successCnt.addAndGet(1);
                } catch (Throwable e) {
                    failCnt.addAndGet(1);
                    log.error("Get client fail", e);
                }
            });
        }

        executorService.shutdown();

        Thread.sleep(1000);
        server.stop();
        log.info("Old server stopped");
        new Thread(() -> getServer(new TestThriftServiceHandler.Handler2()).serve()).start();
        log.info("New server started");
        Thread.sleep(4000);

        executorService.awaitTermination(1, TimeUnit.MINUTES);

        log.info("{} , {}", successCnt.intValue(), failCnt.intValue());

        assert successCnt.intValue() == times;
        assert failCnt.intValue() == 0;

    }
}
