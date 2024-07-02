package com.edu.quique.sse.core.broker;

public interface BrokerPublisher<T> {
  void publish(String topic, T data);
}
