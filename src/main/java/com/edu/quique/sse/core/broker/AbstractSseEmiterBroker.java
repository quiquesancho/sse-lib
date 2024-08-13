package com.edu.quique.sse.core.broker;

import com.fasterxml.jackson.core.JsonProcessingException;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.MediaType;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

@NoArgsConstructor
@Slf4j
public abstract class AbstractSseEmiterBroker<K, V> {
  final ConcurrentHashMap<String, CopyOnWriteArrayList<SseEmitter>> inMemorySubscriptorDirectory =
      new ConcurrentHashMap();

  protected void appendSubscriptor(String topic, SseEmitter sub) {
    CopyOnWriteArrayList<SseEmitter> eventSubcriptors =
        (CopyOnWriteArrayList) this.inMemorySubscriptorDirectory.get(topic);
    if (eventSubcriptors == null) {
      eventSubcriptors = new CopyOnWriteArrayList();
      this.inMemorySubscriptorDirectory.put(topic, eventSubcriptors);
    }

    eventSubcriptors.add(sub);
    log.info("New Subscriptor added to topic " + topic);
  }

  protected void removeSubscriptor(String topic, SseEmitter sub) {
    CopyOnWriteArrayList<SseEmitter> subscriptors =
        (CopyOnWriteArrayList) this.inMemorySubscriptorDirectory.get(topic);
    subscriptors.remove(sub);
  }

  protected void onSubscriptionExpiration(String topic, SseEmitter subcriptor) {
    log.info("Expired Subscription event on topic {} , Subscription {}", topic, subcriptor);
    this.removeSubscriptor(topic, subcriptor);
  }

  protected void send(K messageId, SseEmitter subcriptor, V data) {
    try {
      SseEmitter.SseEventBuilder builder =
          SseEmitter.event()
              .data(this.serializePayload(data), MediaType.APPLICATION_JSON)
              .id(this.serializeMessageId(messageId));
      subcriptor.send(builder);
    } catch (IOException e) {
      log.error(e.getMessage());
    }
  }

  protected void broadCastMessage(K messageId, String topic, V message) {
    CopyOnWriteArrayList<SseEmitter> subscriptors =
        (CopyOnWriteArrayList) this.inMemorySubscriptorDirectory.get(topic);
    if (subscriptors != null) {
      subscriptors.parallelStream()
          .forEach(
              (sub) -> {
                try {
                  this.send(messageId, sub, message);
                } catch (IllegalStateException e) {
                  this.removeSubscriptor(topic, sub);
                  log.info(
                      "Found orphan connection on topic {}, removed subscription success.", topic);
                }
              });
    }
  }

  protected abstract String serializeMessageId(K key) throws JsonProcessingException;

  protected abstract String serializePayload(V payload) throws JsonProcessingException;
}
