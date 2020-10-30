package com.mbs.spark.converts;

import com.google.gson.reflect.TypeToken;
import com.mbs.spark.tools.JsonTool;

import javax.persistence.AttributeConverter;
import java.util.Map;

public class MapConvert<K, V> implements AttributeConverter<Map<K, V>, String> {

    @Override
    public String convertToDatabaseColumn(Map<K, V> attribute) {
        return JsonTool.to(attribute);
    }

    @Override
    public Map<K, V> convertToEntityAttribute(String dbData) {
        return JsonTool.from(dbData, new TypeToken<Map<K, V>>(){});
    }
}
