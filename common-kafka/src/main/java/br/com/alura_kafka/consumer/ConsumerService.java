package br.com.alura_kafka.consumer;

import br.com.alura_kafka.Message;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface ConsumerService<T> {
    String getConsumerGroup();
    String getTopic();
    void parse(ConsumerRecord<String, Message<T>> record) throws Exception ;
}
