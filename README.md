# thrift4j
Thrift utils for Java

## Usage

```java
TransportConfig transportConfig = new TcpTransportConfig(
  "localhost", DEFAULT_SERVER_PORT, 200);
ClientPoolConfig poolConfig = new ClientPoolConfig(3);

ClientPool<EchoService.Client> pool = ClientPool.<EchoService.Client>builder()
  .poolConfig(poolConfig).transportConfig(transportConfig)
  .serviceClientClazz(EchoService.Client.class)
  .clientFactory(t -> new EchoService.Client(new TBinaryProtocol(new TFramedTransport(t))))
  .validator(c -> Try.of(() -> c.echo("ping").equals("ping")).getOrElse(false))
  .build();

EchoService.Iface client = pool.getClient();
String response = client.echo("hello world");
assert "hello world".equals(response);
```

