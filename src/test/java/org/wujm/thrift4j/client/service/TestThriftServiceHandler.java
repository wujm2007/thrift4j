package org.wujm.thrift4j.client.service;

public class TestThriftServiceHandler implements TestThriftService.Iface {

    @Override
    public String echo(String message) {
        return message;
    }

}
