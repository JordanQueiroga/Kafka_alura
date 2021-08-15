package br.com.alura_kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class BatchSendMessageService {

    private final Connection connection;

    BatchSendMessageService() throws SQLException {
        String url = "jdbc:sqlite:target/users_database.db";
        connection = DriverManager.getConnection(url);
        try {
            connection.createStatement().execute("create table Users (uuid varchar(200) primary key, email varchar(200))");
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
    }

    public static void main(String[] args) throws InterruptedException, SQLException, ExecutionException {
        var batchService = new BatchSendMessageService();
        try (var service = new KafkaService(BatchSendMessageService.class.getName(),
                "ECOMMERCE_SEND_MESSAGE_TO_ALL_USERS",
                batchService::parse,
                Map.of())
        ) {
            service.run();
        }
    }

    private final KafkaDispatcher userDispatcher = new KafkaDispatcher<User>();

    private void parse(ConsumerRecord<String, Message<String>> record) throws SQLException, ExecutionException, InterruptedException {
        System.out.println("--------------------------");
        System.out.println("Process new batch");
        System.out.println("Topic: " + record.value());
        var payload = record.value().getPayload();
        for (User user : getAllUsers()) {
            userDispatcher.sendAsync(payload, user.getUuid(), record.value().getCorrelationId().continueWith(BatchSendMessageService.class.getSimpleName()), user);
            System.out.println("Enviei para o " + user);
        }
    }

    private List<User> getAllUsers() throws SQLException {
        ResultSet results = connection.prepareStatement("select uuid from Users").executeQuery();
        List<User> users = new ArrayList<>();
        while (results.next()) {
            users.add(new User(results.getString(1)));
        }
        return users;
    }

}
