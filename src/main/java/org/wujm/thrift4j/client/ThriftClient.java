package org.wujm.thrift4j.client;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.thrift.TServiceClient;

import java.io.Closeable;
import java.io.IOException;

@Slf4j
@RequiredArgsConstructor
public class ThriftClient<T extends TServiceClient> implements Closeable {
    private final T client;

    @Override
    public void close() throws IOException {
    }
}
