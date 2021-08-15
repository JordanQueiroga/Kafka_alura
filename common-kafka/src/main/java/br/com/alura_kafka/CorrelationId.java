package br.com.alura_kafka;

import lombok.Data;

import java.util.UUID;

@Data
public class CorrelationId {

    private final String id;

    public CorrelationId(String title) {
        this.id = title + "(" + UUID.randomUUID() + ")";
    }

    public CorrelationId continueWith(String title) {
        return new CorrelationId(this.id + "-" + title);
    }

}
