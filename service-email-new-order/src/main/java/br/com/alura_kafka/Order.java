package br.com.alura_kafka;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.math.BigDecimal;

@Data
@AllArgsConstructor
public class Order {
    private final String email, orderId;
    private final BigDecimal amount;

}
