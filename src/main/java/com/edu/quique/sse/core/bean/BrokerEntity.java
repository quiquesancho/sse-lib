package com.edu.quique.sse.core.bean;

import lombok.AllArgsConstructor;
import lombok.Getter;


@Getter
@AllArgsConstructor
public abstract class BrokerEntity<K, T> {
  K messageId;
  T payload;
}
