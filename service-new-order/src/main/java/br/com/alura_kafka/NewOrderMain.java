package br.com.alura_kafka;

import br.com.alura_kafka.dispatcher.KafkaDispatcher;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

@Slf4j
public class NewOrderMain {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        try (KafkaDispatcher orderDispatcher = new KafkaDispatcher<Order>()) {
            for (int i = 0; i < 10; i++) {

                String orderId = UUID.randomUUID().toString();
                BigDecimal amount = new BigDecimal(Math.random() * 5000 + 1);
                String email = Math.random() + "@email.com";

                Order order = new Order(orderId, amount, email);

                var sendEmailValue = "Thank for your order!";

                var id = new CorrelationId(NewOrderMain.class.getSimpleName());

                orderDispatcher.send("ECOMMERCE_NEW_ORDER", email, id, order);
            }
        }

    }


}
