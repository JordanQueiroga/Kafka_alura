package br.com.alura_kafka;

import br.com.alura_kafka.consumer.KafkaService;
import br.com.alura_kafka.dispatcher.KafkaDispatcher;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.math.BigDecimal;
import java.util.Map;
import java.util.concurrent.ExecutionException;

@Slf4j
public class FraudDetectorService {

    public static void main(String[] args) throws InterruptedException, ExecutionException {
        var fraudDetectorService = new FraudDetectorService();
        try (var service = new KafkaService(FraudDetectorService.class.getName(),
                "ECOMMERCE_NEW_ORDER",
                fraudDetectorService::parse,
                Map.of())
        ) {
            service.run();
        }
    }

    private final KafkaDispatcher<Order> orderDispatcher = new KafkaDispatcher<>();

    private void parse(ConsumerRecord<String, Message<Order>> record) throws ExecutionException, InterruptedException {
        System.out.println("Process new order, checking for fraud");
        System.out.println(record.key());
        System.out.println(record.value());
        System.out.println(record.partition());
        System.out.println(record.offset());

        if (isFraoud(record.value().getPayload())) {
            System.out.println("Order is a fraud!!!");
            orderDispatcher.send("ECOMMERCE_ORDER_REJECTED",
                    record.value().getPayload().getEmail(),
                    record.value().getCorrelationId().continueWith(FraudDetectorService.class.getSimpleName()),
                    record.value().getPayload());
        } else {
            System.out.println("Oder approved");
            orderDispatcher.send("ECOMMERCE_ORDER_APPROVED", record.value().getPayload().getEmail(),
                    record.value().getCorrelationId().continueWith(FraudDetectorService.class.getSimpleName()),
                    record.value().getPayload());
        }

        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("Order processed");
    }

    private boolean isFraoud(Order order) {
        return order.getAmount().compareTo(new BigDecimal("4000")) >= 0;
    }

}
