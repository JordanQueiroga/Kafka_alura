package br.com.alura_kafka;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.math.BigDecimal;

@Data
@AllArgsConstructor
public class Order {
    private final String orderId;
    private final BigDecimal amount;
    private String email;

}
