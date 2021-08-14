package br.com.alura_kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.Closeable;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

@Slf4j
public class KafkaDispatcher<T> implements Closeable {
    KafkaProducer<String, T> producer;

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

    public void send(String topicName, String key, T value) throws ExecutionException, InterruptedException {
        ProducerRecord<String, T> record = new ProducerRecord(topicName, key, value);

        Callback callback = (data, ex) -> {
            if (ex != null) {
                ex.printStackTrace();
                return;
            }
            log.info("sucesso... nome= {} ::: partition={}/ offset= {}", data.topic(), data.partition(), data.offset());
        };
        producer.send(record, callback).get();
    }

    @Override
    public void close() {
        producer.close();
    }
}
