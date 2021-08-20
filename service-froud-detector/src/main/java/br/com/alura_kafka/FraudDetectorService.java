package br.com.alura_kafka;

import br.com.alura_kafka.consumer.ConsumerService;
import br.com.alura_kafka.consumer.ServiceRunner;
import br.com.alura_kafka.dispatcher.KafkaDispatcher;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.ExecutionException;

@Slf4j
public class FraudDetectorService implements ConsumerService<Order> {

    private final LocalDatabase database;

    public FraudDetectorService() throws SQLException {
        this.database = new LocalDatabase("frauds_database");
        database.createIfNotExist("create table Orders (" +
                "uuid varchar(200) primary key, " +
                "is_fraud boolean" +
                ")");
    }

    public static void main(String[] args) {
        new ServiceRunner(FraudDetectorService::new).start(1);
    }

    private final KafkaDispatcher<Order> orderDispatcher = new KafkaDispatcher<>();

    @Override
    public String getConsumerGroup() {
        return FraudDetectorService.class.getSimpleName();
    }

    @Override
    public String getTopic() {
        return "ECOMMERCE_NEW_ORDER";
    }

    @Override
    public void parse(ConsumerRecord<String, Message<Order>> record) throws ExecutionException, InterruptedException, SQLException {
        System.out.println("Process new order, checking for fraud");
        System.out.println(record.key());
        System.out.println(record.value());
        System.out.println(record.partition());
        System.out.println(record.offset());

        Order order = record.value().getPayload();

        if (hasProcessed(order)) {
            System.out.println("Order " + order.getOrderId() + " was already processed");
        }

        if (isFraoud(order)) {
            database.update("insert into Orders (uuid,is_fraud) values (?, true)", order.getOrderId());
            System.out.println("Order is a fraud!!!");
            orderDispatcher.send("ECOMMERCE_ORDER_REJECTED",
                    order.getEmail(),
                    record.value().getCorrelationId().continueWith(FraudDetectorService.class.getSimpleName()),
                    order);
        } else {
            database.update("insert into Orders (uuid,is_fraud) values (?, false)", order.getOrderId());
            System.out.println("Oder approved");
            orderDispatcher.send("ECOMMERCE_ORDER_APPROVED", order.getEmail(),
                    record.value().getCorrelationId().continueWith(FraudDetectorService.class.getSimpleName()),
                    order);
        }

        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("Order processed");
    }

    private boolean hasProcessed(Order order) throws SQLException {
        ResultSet results = database.query("select uuid from Orders where uuid = ? limit 1", order.getOrderId());
        return results.next();
    }

    private boolean isFraoud(Order order) {
        return order.getAmount().compareTo(new BigDecimal("4000")) >= 0;
    }

}
