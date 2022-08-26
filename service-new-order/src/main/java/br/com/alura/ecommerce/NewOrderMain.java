
package br.com.alura.ecommerce;

import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        try(var orderDispatcher = new KafkaDispatcher<Order>()) {
            try(var emailDispatcher = new KafkaDispatcher<Email>()) {
                for (var i = 0; i < 2; i++) {
                    var key = UUID.randomUUID().toString();
                    var userId = UUID.randomUUID().toString();
                    var orderId = UUID.randomUUID().toString();
                    var amount = new BigDecimal(Math.random() *500 +1);
                    var order = new Order (userId,orderId,amount);
                    orderDispatcher.send("ECOMMERCE_NEW_ORDER", userId, order);

                    var email = new Email("Thank you for your order! We are processing your order!","bode");
                    emailDispatcher.send("ECOMMERCE_SEND_EMAIL", key, email);
                }
            }
        }
    }
}
