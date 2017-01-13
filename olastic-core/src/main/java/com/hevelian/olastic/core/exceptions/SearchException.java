package com.hevelian.olastic.core.exceptions;

/**
 * Custom search exception.
 * 
 * @author rdidyk
 */
public class SearchException extends RuntimeException {

    private static final long serialVersionUID = 29521155702806329L;

    /**
     * Constructor that accepts error message.
     * 
     * @param message
     *            error message
     */
    public SearchException(String message) {
        super(message);
    }
}
