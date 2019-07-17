package org.wujm.thrift4j.client;

import lombok.extern.slf4j.Slf4j;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransportException;
import org.junit.BeforeClass;
import org.junit.Test;
import org.wujm.thrift4j.client.service.TestThriftService;
import org.wujm.thrift4j.client.service.TestThriftServiceHandler;
import org.wujm.thrift4j.client.transport.TcpTransportConfig;
import org.wujm.thrift4j.client.transport.TransportConfig;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class TestThriftClientPool {

    private static final int DEFAULT_SERVER_PORT = 9090;

    private static TSimpleServer getServer(TestThriftService.Iface iface, TServerSocket serverTransport) {
        TServer.Args processor = new TSimpleServer.Args(serverTransport)
                .inputTransportFactory(new TFramedTransport.Factory())
                .outputTransportFactory(new TFramedTransport.Factory())
                .processor(new TestThriftService.Processor<>(iface));
        return new TSimpleServer(processor);
    }

    @BeforeClass
    public static void setUpServer() throws InterruptedException, TTransportException {
        TServerSocket serverTransport = new TServerSocket(DEFAULT_SERVER_PORT);
        new Thread(() -> getServer(new TestThriftServiceHandler.Handler0(), serverTransport).serve()).start();
        /* Wait for server to start */
        Thread.sleep(1000);
    }

    @Test
    public void testEcho() throws InterruptedException {
        TransportConfig transportConfig = new TcpTransportConfig("localhost", DEFAULT_SERVER_PORT, 200);
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
    public void testServerReset() throws InterruptedException, TTransportException {
        int serverPort = DEFAULT_SERVER_PORT + 1;
        TServerSocket serverTransport = new TServerSocket(serverPort);
        TSimpleServer server = getServer(new TestThriftServiceHandler.Handler1(), serverTransport);

        Thread serverThread1 = new Thread(() -> {
            log.info("Server 1 start");
            server.serve();
        }, "server-thread-1");
        serverThread1.start();

        TransportConfig transportConfig = new TcpTransportConfig("localhost", serverPort, 1000);
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
                    Thread.sleep((counter >= times / 2 ? counter + 50 : counter) * 100);
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

        /* Wait for first half to complete */
        Thread.sleep(1000);
        server.stop();
        log.info("Old server stopped");
        TServerSocket newServerTransport = new TServerSocket(serverPort);
        new Thread(() -> {
            log.info("Server 2 start");
            getServer(new TestThriftServiceHandler.Handler2(), newServerTransport).serve();
        }, "server-thread-2").start();
        log.info("New server started");
        Thread.sleep(4000);

        executorService.awaitTermination(30, TimeUnit.SECONDS);

        assert successCnt.intValue() == times;
        assert failCnt.intValue() == 0;

    }
}
