package org.wujm.thrift4j.exception;

/**
 * @author wujunmin
 */
public class ThriftException extends RuntimeException {

    public ThriftException() {
        super();
    }

    public ThriftException(String message, Throwable cause) {
        super(message, cause);
    }

    public ThriftException(String message) {
        super(message);
    }
}

