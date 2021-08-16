package br.com.alura_kafka.consumer;

public interface ServiceFactory<T> {
    ConsumerService<T> create();
}
