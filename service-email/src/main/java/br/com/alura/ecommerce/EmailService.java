package br.com.alura.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.HashMap;

public class EmailService {
    public static void main(String[] args) {
        var emailService = new EmailService();
        try(var service = new KafkaService(EmailService.class.getSimpleName()
                ,"ECOMMERCE_SEND_EMAIL"
                , emailService::parse
                , Email.class
                ,new HashMap<>())) {
            service.run();
        }
    }

    private void parse(ConsumerRecord<String,Email> record) {
        System.out.println("----------------------------------------------");
        System.out.println("Enviando email");
        System.out.println(record.key());
        System.out.println(record.value());
        System.out.println(record.partition());
        System.out.println(record.offset());
        try {
            Thread.sleep(5000);

        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("Email Enviado");
    }
}
