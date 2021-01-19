package com.mbs.spark.converts;

import com.google.gson.reflect.TypeToken;
import com.mbs.spark.utils.JsonUtil;

import javax.persistence.AttributeConverter;

public class JsonConvert<K> implements AttributeConverter<K, String> {

    @Override
    public String convertToDatabaseColumn(K attribute) {
        return JsonUtil.to(attribute);
    }

    @Override
    public K convertToEntityAttribute(String dbData) {
        return JsonUtil.from(dbData, new TypeToken<K>(){});
    }
}
