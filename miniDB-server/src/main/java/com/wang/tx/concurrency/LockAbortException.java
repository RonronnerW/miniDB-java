package com.wang.tx.concurrency;

public class LockAbortException extends RuntimeException {
    public LockAbortException() {
    }

    public LockAbortException(String message) {
        super(message);
    }

    public LockAbortException(String message, Throwable cause) {
        super(message, cause);
    }

    public LockAbortException(Throwable cause) {
        super(cause);
    }

    public LockAbortException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}