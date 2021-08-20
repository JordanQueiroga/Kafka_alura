package br.com.alura_kafka;

import br.com.alura_kafka.consumer.ConsumerService;
import br.com.alura_kafka.consumer.ServiceRunner;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.sql.SQLException;
import java.util.UUID;

public class CreateUserService implements ConsumerService<Order> {

    private final  LocalDatabase database;

    public CreateUserService() throws SQLException {
        this.database = new LocalDatabase("users_database");
        database.createIfNotExist("create table Users (uuid varchar(200) primary key, email varchar(200))");
    }

    public static void main(String[] args) {
        new ServiceRunner(CreateUserService::new).start(1);
    }

    @Override
    public String getConsumerGroup() {
        return CreateUserService.class.getSimpleName();
    }

    @Override
    public String getTopic() {
        return "ECOMMERCE_NEW_ORDER";
    }

    @Override
    public void parse(ConsumerRecord<String, Message<Order>> record) throws SQLException {
        System.out.println("Process create user");
        System.out.println(record.key());
        System.out.println(record.value());
        System.out.println(record.partition());
        System.out.println(record.offset());

        var order = record.value();
        if (isNewUser(order.getPayload().getEmail())) {
            insertNewUser(order.getPayload().getEmail());
            System.out.println("User created");
        } else {
            System.out.println("User already exist");
        }

        System.out.println("--------------------");
    }

    private boolean isNewUser(String email) throws SQLException {
        var results = database.query("select uuid from Users where email = ? limit 1", email);
        return !results.next();
    }


    private void insertNewUser(String email) throws SQLException {
        String uuid = UUID.randomUUID().toString();
        database.update("insert into Users (uuid,email) values (?,?)", uuid,email);
        System.out.println("Usuário uuid - " + email + " adicionado");
    }

}
