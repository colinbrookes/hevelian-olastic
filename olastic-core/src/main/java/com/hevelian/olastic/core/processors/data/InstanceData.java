package com.hevelian.olastic.core.processors.data;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.experimental.FieldDefaults;

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
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
@Getter
public class InstanceData<T, V> {

    T type;
    V value;

}
