import br.com.alura.ecommerce.Message;
import br.com.alura.ecommerce.consumer.ConsumerService;
import br.com.alura.ecommerce.consumer.ServiceRunner;
import br.com.alura.ecommerce.dispatcher.KafkaDispatcher;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.concurrent.ExecutionException;

public class EmailNewOrderService implements ConsumerService<Order> {

    private final KafkaDispatcher<String> emailDispatcher = new KafkaDispatcher<>();

    public static void main(String[] args) throws InterruptedException {
        new ServiceRunner(EmailNewOrderService::new).start(5);
    }

    public void parse(ConsumerRecord<String, Message<Order>> record) throws ExecutionException, InterruptedException {
        System.out.println("----------------------------------------------");
        System.out.println("Processing new order, preparing email");
        System.out.println(record.value());

        var emailCode = "Thank you for your order! We are processing your order!";
        var order = record.value().getPayload();
        emailDispatcher.send("ECOMMERCE_SEND_EMAIL", order.getEmail()
                ,record.value().getId().continueWith(EmailNewOrderService.class.getSimpleName()), emailCode);
    }

    @Override
    public String getTopic() {
        return "ECOMMERCE_NEW_ORDER";
    }

    @Override
    public String getConsumerGroup() {
        return EmailNewOrderService.class.getSimpleName();
    }

}
