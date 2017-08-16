package com.hevelian.olastic.core.processors.data;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * Instance data represent EDM type and value (value could be: property, entity,
 * entityCollection, etc.).
 * 
 * @author rdidyk
 *
 * @param <T>
 *            edm type class
 * @param <V>
 *            edm value class
 */
@AllArgsConstructor
@Getter
public class InstanceData<T, V> {

    private final T type;
    private final V value;

}
