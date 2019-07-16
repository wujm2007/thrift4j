package org.wujm.thrift4j.client;

public class TestThriftServiceHandler implements TestThriftService.Iface {

    @Override
    public String echo(String message) {
        return message;
    }

}
