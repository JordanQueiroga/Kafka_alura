package br.com.alura_kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Map;

@Slf4j
public class EmailService {

    public static void main(String[] args) throws InterruptedException {
        var emailService = new EmailService();
        try (var service = new KafkaService(EmailService.class.getName(),
                "ECOMMERCE_SEND_EMAIL",
                emailService::parse,
                String.class,
                Map.of())) {
            service.run();
        }
    }

    private void parse(ConsumerRecord<String, String> record) {
        System.out.println("Send email");
        System.out.println("key: " + record.key());
        System.out.println("value: " + record.value());
        System.out.println("Partition: " + record.partition());
        System.out.println("Offset: " + record.offset());

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
        }
        System.out.println("Email send ");
    }


}
