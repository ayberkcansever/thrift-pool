package com.insto.thriftpool.exception;

public class NoBackendServiceException extends ConnectionFailException {

    private static final long serialVersionUID = 8966434958841745191L;

    public NoBackendServiceException() {
        super();
    }

    public NoBackendServiceException(String message) {
        super(message);
    }

}
