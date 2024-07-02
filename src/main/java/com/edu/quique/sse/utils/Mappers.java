package com.edu.quique.sse.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.NoArgsConstructor;

import java.time.OffsetDateTime;

@NoArgsConstructor
public class Mappers {
  public static ObjectMapper offsetDateTimeMapper() {
    ObjectMapper objectMapper = new ObjectMapper();
    objectMapper.registerModule(new JavaTimeModule());
    SimpleModule simpleModule = new SimpleModule();
    simpleModule.addSerializer(OffsetDateTime.class, new OffsetDateTimeSerializar());
    objectMapper.registerModule(simpleModule);
    return objectMapper;
  }
}
