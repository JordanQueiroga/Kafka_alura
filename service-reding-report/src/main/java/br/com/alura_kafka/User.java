package br.com.alura_kafka;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.math.BigDecimal;

@Data
@AllArgsConstructor
public class User {
    private final String uuid;

    public String getReportPath() {
        return "target/" + uuid + "-report.txt";
    }
}
