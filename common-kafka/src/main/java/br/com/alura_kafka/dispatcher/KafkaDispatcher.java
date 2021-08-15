package br.com.alura_kafka.dispatcher;

import br.com.alura_kafka.CorrelationId;
import br.com.alura_kafka.Message;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.Closeable;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

@Slf4j
public class KafkaDispatcher<T> implements Closeable {
    KafkaProducer<String, Message<T>> producer;

    public KafkaDispatcher() {
        this.producer = new KafkaProducer<>(properties());
    }


    private static Properties properties() {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, GsonSerializer.class.getName());
        //AKS serve para configurar se a mensagem realmente chegou no kafka e replicou a informação nos N clusteres informado na property
        //Por padrão é zero e aí ele não espera o retorno da resposta podendo assim ter mensagens perdidas
        //All vai esperar as replicas sejam criadas em todos os clusters
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        return properties;
    }

    public void send(String topicName, String key, CorrelationId id, T payload) throws ExecutionException, InterruptedException {
        sendAsync(topicName, key, id, payload).get();
    }

    public Future<RecordMetadata> sendAsync(String topicName, String key, CorrelationId id, T payload) {
        Message<T> value = new Message<>(id.continueWith("_" + topicName), payload);
        ProducerRecord<String, Message<T>> record = new ProducerRecord(topicName, key, value);

        Callback callback = (data, ex) -> {
            if (ex != null) {
                ex.printStackTrace();
                return;
            }
            log.info("sucesso... nome= {} ::: partition={}/ offset= {}", data.topic(), data.partition(), data.offset());
        };

        return producer.send(record, callback);
    }

    @Override
    public void close() {
        producer.close();
    }
}
