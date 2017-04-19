package com.hevelian.olastic.core.common;

import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.elasticsearch.index.mapper.BinaryFieldMapper;
import org.elasticsearch.index.mapper.BooleanFieldMapper;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.mapper.TextFieldMapper;

import lombok.extern.log4j.Log4j2;

/**
 * Class for mapping Elasticsearch types to Edm primitive types.
 * 
 * @author yuflyud
 */
@Log4j2
public class PrimitiveTypeMapper {

    public EdmPrimitiveTypeKind map(String elasticType) {
        EdmPrimitiveTypeKind result;
        if (TextFieldMapper.CONTENT_TYPE.equals(elasticType)
                || KeywordFieldMapper.CONTENT_TYPE.equals(elasticType)) {
            result = EdmPrimitiveTypeKind.String;
        } else if (NumberFieldMapper.NumberType.LONG.typeName().equals(elasticType)) {
            result = EdmPrimitiveTypeKind.Int64;
        } else if (NumberFieldMapper.NumberType.INTEGER.typeName().equals(elasticType)) {
            result = EdmPrimitiveTypeKind.Int32;
        } else if (NumberFieldMapper.NumberType.SHORT.typeName().equals(elasticType)) {
            result = EdmPrimitiveTypeKind.Int16;
        } else if (NumberFieldMapper.NumberType.BYTE.typeName().equals(elasticType)) {
            result = EdmPrimitiveTypeKind.Byte;
        } else if (NumberFieldMapper.NumberType.DOUBLE.typeName().equals(elasticType)
                || NumberFieldMapper.NumberType.FLOAT.typeName().equals(elasticType)) {
            result = EdmPrimitiveTypeKind.Double;
        } else if (DateFieldMapper.CONTENT_TYPE.equals(elasticType)) {
            result = EdmPrimitiveTypeKind.DateTimeOffset;
        } else if (BooleanFieldMapper.CONTENT_TYPE.equals(elasticType)) {
            result = EdmPrimitiveTypeKind.Boolean;
        } else if (BinaryFieldMapper.CONTENT_TYPE.equals(elasticType)) {
            result = EdmPrimitiveTypeKind.Binary;
        } else {
            log.warn("Type '{}' is not supported. Setting String instead.", elasticType);
            result = EdmPrimitiveTypeKind.String;
        }
        return result;
    }
}
