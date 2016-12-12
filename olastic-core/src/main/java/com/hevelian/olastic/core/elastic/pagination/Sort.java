package com.hevelian.olastic.core.elastic.pagination;

/**
 * Sort option for queries. You have to provide at least a list of properties to
 * sort for that must not include {@literal null} or empty strings. The
 * direction defaults to {@link Sort#DEFAULT_DIRECTION}.
 * 
 * @author rdidyk
 */
public class Sort {

    /** Default sort direction. */
    public static final Direction DEFAULT_DIRECTION = Direction.ASC;

    private String property;
    private Direction direction;

    /**
     * Creates a new {@link Sort} instance using the given property with default
     * direction.
     * 
     * @param property
     *            must not be {@literal null}
     */
    public Sort(String property) {
        this(property, DEFAULT_DIRECTION);
    }

    /**
     * Creates a new {@link Sort} instance using the given property with
     * specific {@link Direction} direction.
     * 
     * @param property
     *            must not be {@literal null}
     * @param direction
     *            direction to sort
     */
    public Sort(String property, Direction direction) {
        this.property = property;
        this.direction = direction;
    }

    public Direction getDirection() {
        return direction;
    }

    public String getProperty() {
        return property;
    }

    public void setDirection(Direction direction) {
        this.direction = direction;
    }

    public void setProperty(String property) {
        this.property = property;
    }

    /**
     * Enumeration for sort directions.
     * 
     * @author rdidyk
     */
    public enum Direction {
        /** Ascending direction. */
        ASC,
        /** Descending direction. */
        DESC;

        /**
         * Returns the {@link Direction} enum for the given {@link String}
         * value.
         * 
         * @param value
         *            direction value
         * @throws IllegalArgumentException
         *             in case the given value cannot be parsed into an enum
         *             value.
         * @return direction based on value
         */
        public static Direction fromString(String value) {
            for (Direction direction : values()) {
                if (direction.toString().equalsIgnoreCase(value)) {
                    return direction;
                }
            }
            throw new IllegalArgumentException(String.format(
                    "Invalid value '%s' for orders given! Has to be either 'desc' or 'asc' (case insensitive).",
                    value));
        }
    }

}
