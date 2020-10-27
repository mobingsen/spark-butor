package com.mbs.spark.tools;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import java.lang.reflect.Type;

public final class JsonTool {

    private static final Gson gson = new Gson();

    public static  <T> T from(String src, Type typeOfT) {
        return gson.fromJson(src, typeOfT);
    }

    public static <T> T from(String src, Class<T> classOfT) {
        return gson.fromJson(src, classOfT);
    }

    public static <T> T from(String src, TypeToken<T> typeToken) {
        return gson.fromJson(src, typeToken.getType());
    }

    public static <T> String to(T src) {
        return gson.toJson(src);
    }

    public static Gson gson() {
        return gson;
    }
}
