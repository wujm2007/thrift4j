package org.wujm.thrift4j.client.service;

public class TestThriftServiceHandler {
    public static class Handler0 implements TestThriftService.Iface {
        @Override
        public String echo(String message) {
            return message;
        }
    }

    public static class Handler1 implements TestThriftService.Iface {
        @Override
        public String echo(String message) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return message + " by 1 @" + System.currentTimeMillis() / 1000;
        }
    }

    public static class Handler2 implements TestThriftService.Iface {
        @Override
        public String echo(String message) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return message + " by 2 @" + System.currentTimeMillis() / 1000;
        }
    }

}
