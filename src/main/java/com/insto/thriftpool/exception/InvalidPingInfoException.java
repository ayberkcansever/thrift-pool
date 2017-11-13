package com.insto.thriftpool.exception;

public class InvalidPingInfoException extends ConnectionFailException {

    public InvalidPingInfoException() {
        super();
    }

    public InvalidPingInfoException(String message) {
        super(message);
    }

}
