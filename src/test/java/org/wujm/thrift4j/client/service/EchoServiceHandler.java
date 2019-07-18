package org.wujm.thrift4j.client.service;

public class EchoServiceHandler {
    public static class Handler0 implements EchoService.Iface {
        @Override
        public String echo(String msg) {
            return msg;
        }
    }

    public static class Handler1 implements EchoService.Iface {
        @Override
        public String echo(String msg) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return msg + " by 1 @" + System.currentTimeMillis() / 1000;
        }
    }

    public static class Handler2 implements EchoService.Iface {
        @Override
        public String echo(String msg) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return msg + " by 2 @" + System.currentTimeMillis() / 1000;
        }
    }

}
