package br.com.alura.ecommerce;

import br.com.alura.ecommerce.consumer.ConsumerService;
import br.com.alura.ecommerce.consumer.ServiceRunner;
import br.com.alura.ecommerce.dispatcher.KafkaDispatcher;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.concurrent.ExecutionException;

public class FraudDetectorService implements ConsumerService<Order> {

    private final KafkaDispatcher<Order> orderDispatcher = new KafkaDispatcher<>();

    private final LocalDatabase database;

    public FraudDetectorService() throws SQLException {
        this.database = new LocalDatabase("users_database");
        this.database.createIfNotExists("""
                    create table Orders 
                    (uuid varchar(200) primary key,
                    email varchar(200),
                    is_fraud boolean)
                    """);
    }

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        new ServiceRunner(FraudDetectorService::new).start(1);
    }

    public void parse(ConsumerRecord<String, Message<Order>> record) throws ExecutionException, InterruptedException, SQLException {
        System.out.println("----------------------------------------------");
        System.out.println("Processando novo pedido, checkando fraude");
        System.out.println(record.key());
        System.out.println(record.value());
        System.out.println(record.partition());
        System.out.println(record.offset());
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        var message = record.value();
        var order = message.getPayload();

        if(wasProcessed(order)){
            System.out.println("Order has already been processed");
        }

        if(isFraud(order)){

            database.update("""
                    insert into Orders (uuid,is_fraud) values (?, true)
                    """, order.getOrderId());
            System.out.println("Order is a Fraud");

            orderDispatcher.send("ECOMMERCE_ORDER_REJECTED", order.getEmail(),message.getId().continueWith(FraudDetectorService.class.getSimpleName()), order);
        }else{

            database.update("""
                    insert into Orders (uuid,is_fraud) values (?, false)
                    """, order.getOrderId());
            System.out.println("Aprooved: " + order);
            orderDispatcher.send("ECOMMERCE_ORDER_APPROVED", order.getEmail(),message.getId().continueWith(FraudDetectorService.class.getSimpleName()), order);
        }
        System.out.println("Pedido processado");
    }

    private boolean wasProcessed(Order order) throws SQLException {
        var results = database.query("select uuid from Orders where uuid = ? limit 1", order.getOrderId());
        return results.next();
    }

    @Override
    public String getTopic() {
        return "ECOMMERCE_NEW_ORDER";
    }

    @Override
    public String getConsumerGroup() {
        return FraudDetectorService.class.getSimpleName();
    }

    private boolean isFraud(Order order) {
        return order.getAmount().compareTo(new BigDecimal("4500")) >= 0;
    }
}
