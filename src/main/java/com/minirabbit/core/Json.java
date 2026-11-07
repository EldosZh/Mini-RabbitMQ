package com.minirabbit.core;

import com.fasterxml.jackson.databind.*;

public final class Json {
  private Json() {}

  public static final ObjectMapper MAPPER =
      new ObjectMapper()
          .enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
          .enable(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES)
          .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
          .enable(DeserializationFeature.FAIL_ON_NUMBERS_FOR_ENUMS)
          .enable(com.fasterxml.jackson.core.JsonParser.Feature.STRICT_DUPLICATE_DETECTION);

  public static byte[] bytes(Object value) {
    try {
      return MAPPER.writeValueAsBytes(value);
    } catch (Exception e) {
      throw new IllegalArgumentException("Cannot encode JSON", e);
    }
  }

  public static <T> T read(byte[] data, Class<T> type) {
    try {
      T result = MAPPER.readValue(data, type);
      if (result == null) throw new IllegalArgumentException("JSON null is not a request");
      return result;
    } catch (Exception e) {
      throw new IllegalArgumentException("Invalid JSON for " + type.getSimpleName(), e);
    }
  }
}
