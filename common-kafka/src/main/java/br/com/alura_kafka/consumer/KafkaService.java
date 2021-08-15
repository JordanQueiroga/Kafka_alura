package br.com.alura_kafka.consumer;

import br.com.alura_kafka.Message;
import br.com.alura_kafka.dispatcher.GsonSerializer;
import br.com.alura_kafka.dispatcher.KafkaDispatcher;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.Closeable;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.regex.Pattern;

@Slf4j
public class KafkaService<T> implements Closeable {
    private final KafkaConsumer<String, Message<T>> consumer;
    private ConsumerFunction parse;

    private Consumer<ConsumerRecord<String, String>> parseConsumer;

    public KafkaService(String groupId, String topic, ConsumerFunction<T> parse, Map<String, String> properties) {
        this(parse, groupId, properties);
        consumer.subscribe(Collections.singletonList(topic));
    }

    public KafkaService(String groupId, Pattern topic, ConsumerFunction<T> parse, Map<String, String> properties) {
        this(parse, groupId, properties);
        consumer.subscribe(topic);
    }

    private KafkaService(ConsumerFunction<T> parse, String groupId, Map<String, String> properties) {
        this.consumer = new KafkaConsumer(getProperties(groupId, properties));
        this.parse = parse;
    }

   public void run() throws ExecutionException, InterruptedException {
        try (var deadLetter = new KafkaDispatcher()) {

            while (true) {
                ConsumerRecords<String, Message<T>> records = consumer.poll(Duration.ofMillis(100));
                if (!records.isEmpty()) {
                    for (ConsumerRecord record : records) {
                        try {
                           if(true) throw new RuntimeException("deu erro");

                            parse.consume(record);
                        } catch (Exception e) {
                            log.info("Error when send message!", e);
                            e.printStackTrace();
                            var message = (Message<T>) record.value();
                            deadLetter.send("ECOMMERCE_DEADLETTER",
                                    message.getCorrelationId().toString(),
                                    message.getCorrelationId().continueWith("DeadLetter"),
                                    new GsonSerializer().serialize("", message.toString()));
                        }
                    }
                }
            }
        }
    }

    private Properties getProperties(String groupId, Map<String, String> overrideProperties) {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GsonDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
        properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
        properties.putAll(overrideProperties);

        return properties;
    }

    @Override
    public void close() {
        consumer.close();
    }
}
