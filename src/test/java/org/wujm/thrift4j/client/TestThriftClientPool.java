package org.wujm.thrift4j.client;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.server.TThreadPoolServer.Args;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.junit.BeforeClass;
import org.junit.Test;
import org.wujm.thrift4j.client.transport.TCPTransportConfig;
import org.wujm.thrift4j.client.transport.TransportConfig;

import java.util.Calendar;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class TestThriftClientPool {

    @BeforeClass
    public static void setUp() {
        int port = 9090;

        try {
            TServerTransport serverTransport = new TServerSocket(port);

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
        Calendar down = Calendar.getInstance(), restart = Calendar.getInstance();
        down.add(Calendar.SECOND, 5); // down after 5 secs
        restart.add(Calendar.SECOND, 6); // restart after 6 secs

        TransportConfig transportConfig = new TCPTransportConfig("127.0.0.1", 9090, 1);
        ClientPoolConfig poolConfig = new ClientPoolConfig(3);
        ClientPool<TestThriftService.Client> pool = new ClientPool<>(
                poolConfig,
                new GenericObjectPool<>(new ConnectionFactory(transportConfig)),
                TestThriftService.Client.class,
                transport -> new TestThriftService.Client(new TBinaryProtocol(new TFramedTransport(transport)))
        );

        ExecutorService executorService = Executors.newFixedThreadPool(10);
        AtomicInteger expectedNotFail = new AtomicInteger();

        for (int i = 0; i < 100; i++) {
            int counter = i;
            Thread.sleep(100);
            executorService.submit(() -> {
                try {
                    TestThriftService.Iface client = pool.getClient();
                    String response = client.echo("Hello " + counter + "!");
                    log.info("[testEcho] {} get response: {}", client, response);
                } catch (Throwable e) {
                    expectedNotFail.addAndGet(1);
                    log.error("[testEcho] get client fail", e);
                }
            });
        }

        executorService.shutdown();
        executorService.awaitTermination(1, TimeUnit.MINUTES);

        assert expectedNotFail.intValue() == 0;

    }
}
