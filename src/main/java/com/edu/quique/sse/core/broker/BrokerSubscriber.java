package com.edu.quique.sse.core.broker;

public interface BrokerSubscriber<T> {
    T subscribe(String topic, T subscript);
    void unSubscribe(String topic, T subscript);
}
