package br.com.alura_kafka;

import br.com.alura_kafka.consumer.KafkaService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ExecutionException;

@Slf4j
public class ReadingReportService {

    private final Path SOURCE = new File("src/main/resources/report.txt").toPath();

    public static void main(String[] args) throws InterruptedException, ExecutionException {
        var reportService = new ReadingReportService();
        try (var service = new KafkaService(ReadingReportService.class.getName(),
                "ECOMMERCE_USER_GENERATE_READING_REPORT",
                reportService::parse,
                Map.of())
        ) {
            service.run();
        }
    }


    private void parse(ConsumerRecord<String, Message<User>> record) throws IOException {
        System.out.println("-------------------------------");
        System.out.println("Process report for"+ record.value());

        var user = record.value().getPayload();
        var target = new File(user.getReportPath());
        IO.copyTo(SOURCE,target);
        IO.append(target,"Created for "+user.getReportPath());

        System.out.println("File created: "+target.getAbsolutePath());

    }

}
