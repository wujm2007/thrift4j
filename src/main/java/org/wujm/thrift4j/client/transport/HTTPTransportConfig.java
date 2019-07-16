package org.wujm.thrift4j.client.transport;

import lombok.Data;
import org.apache.thrift.transport.THttpClient;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

/**
 * @author wujunmin
 */
@Data
public class HTTPTransportConfig implements TransportConfig {

    private final String url;
    private final int timeout;

    public HTTPTransportConfig(String url, int timeout) {
        this.url = url;
        this.timeout = timeout;
    }

    @Override
    public TTransport generateTransport() throws TTransportException {
        THttpClient transport = new THttpClient(url);
        transport.setConnectTimeout(timeout);
        return transport;
    }
}
