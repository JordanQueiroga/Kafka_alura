package br.com.alura_kafka;

import br.com.alura_kafka.consumer.ConsumerService;
import br.com.alura_kafka.consumer.KafkaService;
import br.com.alura_kafka.consumer.ServiceRunner;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ExecutionException;

@Slf4j
public class ReadingReportService implements ConsumerService<User> {

    private final Path SOURCE = new File("src/main/resources/report.txt").toPath();

    public static void main(String[] args) throws InterruptedException, ExecutionException {
        new ServiceRunner(ReadingReportService::new).start(4);
    }


    public void parse(ConsumerRecord<String, Message<User>> record) throws Exception {
        System.out.println("-------------------------------");
        System.out.println("Process report for" + record.value());

        var user = record.value().getPayload();
        var target = new File(user.getReportPath());
        IO.copyTo(SOURCE, target);
        IO.append(target, "Created for " + user.getReportPath());

        System.out.println("File created: " + target.getAbsolutePath());

    }

    @Override
    public String getConsumerGroup() {
        return ReadingReportService.class.getSimpleName();
    }

    @Override
    public String getTopic() {
        return "ECOMMERCE_USER_GENERATE_READING_REPORT";
    }
}
