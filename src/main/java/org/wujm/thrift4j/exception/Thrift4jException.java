package org.wujm.thrift4j.exception;

/**
 * @author wujunmin
 */
public class Thrift4jException extends RuntimeException {

    public Thrift4jException() {
        super();
    }

    public Thrift4jException(String message, Throwable cause) {
        super(message, cause);
    }

    public Thrift4jException(String message) {
        super(message);
    }
}

