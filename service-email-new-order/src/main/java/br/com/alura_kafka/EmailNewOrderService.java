package br.com.alura_kafka;

import br.com.alura_kafka.consumer.KafkaService;
import br.com.alura_kafka.dispatcher.KafkaDispatcher;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Map;
import java.util.concurrent.ExecutionException;

@Slf4j
public class EmailNewOrderService {

    public static void main(String[] args) throws InterruptedException, ExecutionException {
        var emailService = new EmailNewOrderService();
        try (var service = new KafkaService(EmailNewOrderService.class.getName(),
                "ECOMMERCE_NEW_ORDER",
                emailService::parse,
                Map.of())
        ) {
            service.run();
        }
    }

    private final KafkaDispatcher<String> emailDispatcher = new KafkaDispatcher<>();

    private void parse(ConsumerRecord<String, Message<Order>> record) throws ExecutionException, InterruptedException {
        System.out.println("Process new order, preparing email");
        Message<Order> message = record.value();
        System.out.println(message);

        String emailCode = "texto do email";
        Order order = message.getPayload();
        CorrelationId id = message.getCorrelationId().continueWith(EmailNewOrderService.class.getSimpleName());

        emailDispatcher.send("ECOMMERCE_SEND_EMAIL", order.getEmail(), id, emailCode);
    }

}
