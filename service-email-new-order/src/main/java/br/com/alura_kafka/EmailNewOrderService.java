package br.com.alura_kafka;

import br.com.alura_kafka.consumer.ConsumerService;
import br.com.alura_kafka.consumer.ServiceRunner;
import br.com.alura_kafka.dispatcher.KafkaDispatcher;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.concurrent.ExecutionException;

@Slf4j
public class EmailNewOrderService implements ConsumerService<Order> {

    public static void main(String[] args) {
        new ServiceRunner(EmailNewOrderService::new).start(1);
    }

    @Override
    public String getConsumerGroup() {
        return EmailNewOrderService.class.getSimpleName();
    }

    @Override
    public String getTopic() {
        return "ECOMMERCE_NEW_ORDER";
    }

    private final KafkaDispatcher<String> emailDispatcher = new KafkaDispatcher<>();

    public void parse(ConsumerRecord<String, Message<Order>> record) throws ExecutionException, InterruptedException {
        System.out.println("Process new order, preparing email");
        Message<Order> message = record.value();
        System.out.println(message);

        String emailCode = "texto do email";
        Order order = message.getPayload();
        CorrelationId id = message.getCorrelationId().continueWith(EmailNewOrderService.class.getSimpleName());

        emailDispatcher.send("ECOMMERCE_SEND_EMAIL", order.getEmail(), id, emailCode);
    }

}
