package net.java21.data2flow.ingress;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/** data2flow-ingress: 외부 MQTT 구독, Webhook·엣지 수신 → RabbitMQ data2flow.raw 스트림 기록(confirm 후 ACK) */
@SpringBootApplication
public class IngressApplication {

    public static void main(String[] args) {
        SpringApplication.run(IngressApplication.class, args);
    }
}
