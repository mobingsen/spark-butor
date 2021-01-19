package com.mbs.spark.utils;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import java.lang.reflect.Type;

public final class JsonUtil {

    private static final Gson GSON = new Gson();

    public static  <T> T from(String src, Type typeOfT) {
        return GSON.fromJson(src, typeOfT);
    }

    public static <T> T from(String src, Class<T> classOfT) {
        return GSON.fromJson(src, classOfT);
    }

    public static <T> T from(String src, TypeToken<T> typeToken) {
        return GSON.fromJson(src, typeToken.getType());
    }

    public static <T> String to(T src) {
        return GSON.toJson(src);
    }

    public static Gson gson() {
        return GSON;
    }

    /*################################################pretty gson######################################################*/
    private static final Gson PRETTY_GSON = new Gson()
            .newBuilder()
            .setPrettyPrinting()
            .create();

    public static  <T> T fromPretty(String src, Type typeOfT) {
        return PRETTY_GSON.fromJson(src, typeOfT);
    }

    public static <T> T fromPretty(String src, Class<T> classOfT) {
        return PRETTY_GSON.fromJson(src, classOfT);
    }

    public static <T> T fromPretty(String src, TypeToken<T> typeToken) {
        return PRETTY_GSON.fromJson(src, typeToken.getType());
    }

    public static <T> String toPretty(T src) {
        return PRETTY_GSON.toJson(src);
    }

    public static Gson prettyGson() {
        return PRETTY_GSON;
    }
}
