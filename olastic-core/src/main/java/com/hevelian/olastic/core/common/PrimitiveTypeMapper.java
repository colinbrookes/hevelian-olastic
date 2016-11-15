package com.hevelian.olastic.core.common;

import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.elasticsearch.index.mapper.core.BinaryFieldMapper;
import org.elasticsearch.index.mapper.core.BooleanFieldMapper;
import org.elasticsearch.index.mapper.core.ByteFieldMapper;
import org.elasticsearch.index.mapper.core.DateFieldMapper;
import org.elasticsearch.index.mapper.core.DoubleFieldMapper;
import org.elasticsearch.index.mapper.core.FloatFieldMapper;
import org.elasticsearch.index.mapper.core.IntegerFieldMapper;
import org.elasticsearch.index.mapper.core.LongFieldMapper;
import org.elasticsearch.index.mapper.core.ShortFieldMapper;
import org.elasticsearch.index.mapper.core.StringFieldMapper;

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
        if (StringFieldMapper.CONTENT_TYPE.equals(elasticType)) {
            result = EdmPrimitiveTypeKind.String;
        } else if (LongFieldMapper.CONTENT_TYPE.equals(elasticType)) {
            result = EdmPrimitiveTypeKind.Int64;
        } else if (IntegerFieldMapper.CONTENT_TYPE.equals(elasticType)) {
            result = EdmPrimitiveTypeKind.Int32;
        } else if (ShortFieldMapper.CONTENT_TYPE.equals(elasticType)) {
            result = EdmPrimitiveTypeKind.Int16;
        } else if (ByteFieldMapper.CONTENT_TYPE.equals(elasticType)) {
            result = EdmPrimitiveTypeKind.Byte;
        } else if (DoubleFieldMapper.CONTENT_TYPE.equals(elasticType)) {
            result = EdmPrimitiveTypeKind.Double;
        } else if (FloatFieldMapper.CONTENT_TYPE.equals(elasticType)) {
            result = EdmPrimitiveTypeKind.Decimal;
        } else if (DateFieldMapper.CONTENT_TYPE.equals(elasticType)) {
            result = EdmPrimitiveTypeKind.Date;
        } else if (BooleanFieldMapper.CONTENT_TYPE.equals(elasticType)) {
            result = EdmPrimitiveTypeKind.Boolean;
        } else if (BinaryFieldMapper.CONTENT_TYPE.equals(elasticType)) {
            result = EdmPrimitiveTypeKind.Binary;
        } else {
            log.warn("Type '{1}' not supported. Setting String instead.", elasticType);
            result = EdmPrimitiveTypeKind.String;
        }
        return result;
    }
}
