package br.com.alura_kafka;

import br.com.alura_kafka.consumer.KafkaService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Map;
import java.util.concurrent.ExecutionException;

@Slf4j
public class EmailService {

    public static void main(String[] args) throws InterruptedException, ExecutionException {
        var emailService = new EmailService();
        try (var service = new KafkaService(EmailService.class.getName(),
                "ECOMMERCE_SEND_EMAIL",
                emailService::parse,
                Map.of())) {
            service.run();
        }
    }

    private void parse(ConsumerRecord<String, Message<String>> record) {
        System.out.println("Send email");
        System.out.println("key: " + record.key());
        System.out.println("value: " + record.value().getPayload());
        System.out.println("Partition: " + record.partition());
        System.out.println("Offset: " + record.offset());

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
        }
        System.out.println("Email send ");
    }


}
