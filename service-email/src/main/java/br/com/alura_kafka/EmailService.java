package br.com.alura_kafka;

import br.com.alura_kafka.consumer.ConsumerService;
import br.com.alura_kafka.consumer.ServiceRunner;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;

@Slf4j
public class EmailService implements ConsumerService<String> {

    public static void main(String[] args) {
        new ServiceRunner(EmailService::new).start(4);
    }

    @Override
    public String getConsumerGroup(){
        return EmailService.class.getSimpleName();
    }

    @Override
    public String getTopic(){
        return "ECOMMERCE_SEND_EMAIL";
    }

    @Override
    public void parse(ConsumerRecord<String, Message<String>> record) {
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
