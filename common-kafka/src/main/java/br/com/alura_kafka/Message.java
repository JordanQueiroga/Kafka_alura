package br.com.alura_kafka;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@Data
@Slf4j
public class Message<T> {
    private final CorrelationId correlationId;
    private final T payload;

    public Message(CorrelationId correlationId, T payload) {
        this.correlationId = correlationId;
        this.payload = payload;
    }

}
