package com.edu.quique.sse.impl.broker;

import com.edu.quique.sse.core.bean.BrokerEntity;
import com.edu.quique.sse.core.broker.AbstractSseEmiterBroker;
import com.edu.quique.sse.core.broker.BrokerPublisher;
import com.edu.quique.sse.core.broker.BrokerSubscriber;
import com.edu.quique.sse.utils.Mappers;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;

@Slf4j
public class SseEmiterBroker extends AbstractSseEmiterBroker<OffsetDateTime, BrokerEntity>
    implements BrokerPublisher<BrokerEntity>, BrokerSubscriber<SseEmitter> {
  ObjectMapper mapper;

  public SseEmiterBroker() {
    this.mapper = Mappers.offsetDateTimeMapper();
  }

  @Override
  public void publish(String topic, BrokerEntity data) {
    OffsetDateTime messageId =
        OffsetDateTime.ofInstant(
            Instant.ofEpochMilli(System.currentTimeMillis()), ZoneId.systemDefault());
    this.broadCastMessage(messageId, topic, data);
  }

  @Override
  public SseEmitter subscribe(String topic, SseEmitter subscript) {
    this.appendSubscriptor(topic, subscript);
    subscript.onCompletion(
        () -> {
          this.onSubscriptionExpiration(topic, subscript);
        });
    log.info("Started from now {}", OffsetDateTime.now());
    return subscript;
  }

  @Override
  public void unSubscribe(String topic, SseEmitter subscript) {
    this.removeSubscriptor(topic, subscript);
  }

  @Override
  protected String serializeMessageId(OffsetDateTime key) throws JsonProcessingException {
    return String.valueOf(key.toInstant().toEpochMilli());
  }

  @Override
  protected String serializePayload(BrokerEntity payload) throws JsonProcessingException {
    return mapper.writeValueAsString(payload);
  }
}
