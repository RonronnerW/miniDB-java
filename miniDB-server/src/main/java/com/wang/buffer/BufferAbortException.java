package com.wang.buffer;

public class BufferAbortException extends RuntimeException {
    public BufferAbortException() {
    }

    public BufferAbortException(String message) {
        super(message);
    }

    public BufferAbortException(String message, Throwable cause) {
        super(message, cause);
    }

    public BufferAbortException(Throwable cause) {
        super(cause);
    }

    public BufferAbortException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}