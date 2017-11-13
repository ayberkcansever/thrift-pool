package com.insto.thriftpool.exception;

public class ConnectionFailException extends ThriftException {

    public ConnectionFailException() {
        super();
    }

    public ConnectionFailException(String message) {
        super(message);
    }

    public ConnectionFailException(String message, Throwable cause) {
        super(message, cause);
    }
}
