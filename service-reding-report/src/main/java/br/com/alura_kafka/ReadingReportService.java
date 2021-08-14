package br.com.alura_kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;

@Slf4j
public class ReadingReportService {

    private final Path SOURCE = new File("src/main/resources/report.txt").toPath();

    public static void main(String[] args) throws InterruptedException {
        var reportService = new ReadingReportService();
        try (var service = new KafkaService<>(ReadingReportService.class.getName(),
                "USER_GENERATE_READING_REPORT",
                reportService::parse,
                User.class,
                Map.of())
        ) {
            service.run();
        }
    }


    private void parse(ConsumerRecord<String, User> record) throws IOException {
        System.out.println("-------------------------------");
        System.out.println("Process report for"+ record.value());

        var user = record.value();
        var target = new File(user.getReportPath());
        IO.copyTo(SOURCE,target);
        IO.append(target,"Created for "+user.getReportPath());

        System.out.println("File created: "+target.getAbsolutePath());

    }

}
