package com.dynamolite;

import java.io.Serializable;

/**
 * Response represents a server response to a client request.
 */
public class Response implements Serializable {
    private static final long serialVersionUID = 1L;

    public enum Status {
        SUCCESS,
        ERROR,
        NOT_FOUND
    }

    private final Status status;
    private final String message;

    public Response(Status status, String message) {
        this.status = status;
        this.message = message;
    }

    public Status getStatus() {
        return status;
    }

    public String getMessage() {
        return message;
    }

    public boolean isSuccess() {
        return status == Status.SUCCESS;
    }
} 