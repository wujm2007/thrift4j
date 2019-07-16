package org.wujm.thrift4j.client;

import lombok.extern.slf4j.Slf4j;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.server.TThreadPoolServer.Args;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.junit.BeforeClass;
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


    @BeforeClass
    public static void setUp() {
        try {
            TServerTransport serverTransport = new TServerSocket(PORT);

            Args processor = new TThreadPoolServer.Args(serverTransport)
                    .inputTransportFactory(new TFramedTransport.Factory())
                    .outputTransportFactory(new TFramedTransport.Factory())
                    .processor(new TestThriftService.Processor<>(new TestThriftServiceHandler()));
            //            processor.maxWorkerThreads = 20;
            TThreadPoolServer server = new TThreadPoolServer(processor);

            log.info("Starting test server...");
            new Thread(server::serve).start();
            Thread.sleep(1000); // waiting server init
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    @Test
    public void testEcho() throws InterruptedException {
        TransportConfig transportConfig = new TCPTransportConfig("localhost", PORT, 100);
        ClientPoolConfig poolConfig = new ClientPoolConfig(3);
        ClientPool<TestThriftService.Client> pool = new ClientPool<>(
                poolConfig,
                transportConfig,
                TestThriftService.Client.class,
                transport -> new TestThriftService.Client(new TBinaryProtocol(new TFramedTransport(transport)))
        );

        ExecutorService executorService = Executors.newFixedThreadPool(10);
        AtomicInteger successCnt = new AtomicInteger();
        AtomicInteger failCnt = new AtomicInteger();

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
}
