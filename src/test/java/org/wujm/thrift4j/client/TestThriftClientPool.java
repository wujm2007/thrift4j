package org.wujm.thrift4j.client;

import io.vavr.control.Try;
import lombok.extern.slf4j.Slf4j;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.wujm.thrift4j.client.service.EchoService;
import org.wujm.thrift4j.client.service.EchoServiceHandler;
import org.wujm.thrift4j.client.transport.SocketTransportConfig;
import org.wujm.thrift4j.client.transport.TransportConfig;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class TestThriftClientPool {

    private static final int DEFAULT_SERVER_PORT = 9090;

    private static TSimpleServer getServer(EchoService.Iface iface, TServerSocket serverTransport) {
        TServer.Args processor = new TSimpleServer.Args(serverTransport)
                .inputTransportFactory(new TFramedTransport.Factory())
                .outputTransportFactory(new TFramedTransport.Factory())
                .processor(new EchoService.Processor<>(iface));
        return new TSimpleServer(processor);
    }

    @BeforeClass
    public static void setUpServer() throws InterruptedException, TTransportException {
        TServerSocket serverTransport = new TServerSocket(DEFAULT_SERVER_PORT);

        TThreadPoolServer.Args processor = new TThreadPoolServer.Args(serverTransport)
                .inputTransportFactory(new TFramedTransport.Factory())
                .outputTransportFactory(new TFramedTransport.Factory())
                .processor(new EchoService.Processor<>(new EchoServiceHandler.Handler0()));
        TServer server = new TThreadPoolServer(processor);
        new Thread(server::serve).start();
        /* Wait for server to start */
        Thread.sleep(1000);
    }

    private int test(ClientPool<EchoService.Client> pool, int times, int threadPoolSize) throws InterruptedException {
        ExecutorService executorService = Executors.newFixedThreadPool(threadPoolSize);
        AtomicInteger successCnt = new AtomicInteger();

        for (int i = 0; i < times; i++) {
            int counter = i;
            Thread.sleep(20);
            executorService.submit(() -> {
                try {
                    EchoService.Iface client = pool.getClient();
                    String request = "Hello " + counter + "!";
                    String response = client.echo(request);
                    if (request.equals(response)) {
                        successCnt.addAndGet(1);
                    }
                } catch (Throwable e) {
                    log.error("Get client fail", e);
                }
            });
        }

        executorService.shutdown();
        executorService.awaitTermination(1, TimeUnit.MINUTES);

        return successCnt.intValue();
    }


    @Test
    public void testValidator() throws InterruptedException {
        TransportConfig transportConfig = new SocketTransportConfig("localhost", DEFAULT_SERVER_PORT, 200);
        ClientPoolConfig poolConfig = new ClientPoolConfig(3);

        ClientPool<EchoService.Client> badPool = ClientPool.<EchoService.Client>builder()
                .poolConfig(poolConfig).transportConfig(transportConfig)
                .serviceClientClazz(EchoService.Client.class)
                .clientFactory(t -> new EchoService.Client(new TBinaryProtocol(new TFramedTransport(t))))
                .validator(c -> Try.of(() -> c.echo("ping").equals("pong")).getOrElse(false))
                .build();

        ClientPool<EchoService.Client> goodPool = ClientPool.<EchoService.Client>builder()
                .poolConfig(poolConfig).transportConfig(transportConfig)
                .serviceClientClazz(EchoService.Client.class)
                .clientFactory(t -> new EchoService.Client(new TBinaryProtocol(new TFramedTransport(t))))
                .validator(c -> Try.of(() -> c.echo("ping").equals("ping")).getOrElse(false))
                .build();

        int times = 10;
        int successCntBad = test(badPool, times, 3);
        int successCntGood = test(goodPool, times, 3);

        assert successCntBad == 0;
        assert successCntGood == times;

    }

    @Test
    public void testEcho() throws InterruptedException {
        TransportConfig transportConfig = new SocketTransportConfig(
                "localhost", DEFAULT_SERVER_PORT, 200);
        ClientPoolConfig poolConfig = new ClientPoolConfig(3);
        poolConfig.setMaxTotal(10);

        ClientPool<EchoService.Client> pool = ClientPool.<EchoService.Client>builder()
                .poolConfig(poolConfig).transportConfig(transportConfig)
                .serviceClientClazz(EchoService.Client.class)
                .clientFactory(t -> new EchoService.Client(new TBinaryProtocol(new TFramedTransport(t))))
                .validator(c -> Try.of(() -> c.echo("ping").equals("ping")).getOrElse(false))
                .build();

        int times = 100;
        int successCnt = test(pool, times, 10);

        assert successCnt == times;

    }

    @Test
    public void testServerReset() throws InterruptedException, TTransportException {
        int serverPort = DEFAULT_SERVER_PORT + 1;
        TServerSocket serverTransport = new TServerSocket(serverPort);
        TSimpleServer server = getServer(new EchoServiceHandler.Handler1(), serverTransport);

        Thread serverThread1 = new Thread(() -> {
            log.info("Server 1 start");
            server.serve();
        }, "server-thread-1");
        serverThread1.start();

        TransportConfig transportConfig = new SocketTransportConfig("localhost", serverPort, 1000);
        ClientPoolConfig poolConfig = new ClientPoolConfig(3);
        ClientPool<EchoService.Client> pool = new ClientPool<>(
                poolConfig,
                transportConfig,
                EchoService.Client.class,
                transport -> new EchoService.Client(new TBinaryProtocol(new TFramedTransport(transport))),
                c -> Try.of(() -> c.echo("ping")).isSuccess()
        );

        ExecutorService executorService = Executors.newFixedThreadPool(10);
        AtomicInteger successCnt = new AtomicInteger(), failCnt = new AtomicInteger();

        int times = 10;

        for (int i = 0; i < times; i++) {
            int counter = i;
            executorService.submit(() -> {
                try {
                    Thread.sleep((counter >= times / 2 ? counter + 50 : counter) * 100);
                    EchoService.Iface client = pool.getClient();
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


        /* Start a new server */
        TServerSocket newServerTransport = new TServerSocket(serverPort);
        new Thread(() -> {
            log.info("Server 2 start");
            getServer(new EchoServiceHandler.Handler2(), newServerTransport).serve();
        }, "server-thread-2").start();
        log.info("New server started");
        Thread.sleep(4000);

        executorService.awaitTermination(30, TimeUnit.SECONDS);

        assert successCnt.intValue() == times;
        assert failCnt.intValue() == 0;

    }


    @Ignore
    @Test
    public void testExternalService() throws InterruptedException {
        TransportConfig transportConfig = new SocketTransportConfig("localhost", 6000, 100);
        ClientPoolConfig poolConfig = new ClientPoolConfig(3);
        ClientPool<EchoService.Client> pool = new ClientPool<>(
                poolConfig,
                transportConfig,
                EchoService.Client.class,
                transport -> new EchoService.Client(new TBinaryProtocol(transport)),
                c -> Try.of(() -> c.echo("ping").equals("ping")).getOrElse(false)
        );

        ExecutorService executorService = Executors.newFixedThreadPool(10);
        AtomicInteger successCnt = new AtomicInteger(), failCnt = new AtomicInteger();

        int times = 10000;

        for (int i = 0; i < times; i++) {
            int counter = i;
            executorService.submit(() -> {
                try {
                    EchoService.Iface client = pool.getClient();
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
        executorService.awaitTermination(30, TimeUnit.SECONDS);

    }
}
