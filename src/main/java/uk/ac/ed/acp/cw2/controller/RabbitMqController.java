package uk.ac.ed.acp.cw2.controller;


import com.rabbitmq.client.DeliverCallback;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.http.ResponseEntity; //CW
import org.springframework.web.bind.annotation.*;
import uk.ac.ed.acp.cw2.data.RuntimeEnvironment;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * RabbitMqController is a REST controller that provides endpoints for sending and receiving stock symbols
 * through RabbitMQ. This class interacts with a RabbitMQ environment which is configured dynamically during runtime.
 */
@RestController()
@RequestMapping("/rabbitmq")
public class RabbitMqController {

    private static final String STUDENT_UID = "s2687419"; // replace with your UID
    private static final Logger logger = LoggerFactory.getLogger(RabbitMqController.class);
    private final RuntimeEnvironment environment;
    private final String[] stockSymbols = "AAPL,MSFT,GOOG,AMZN,TSLA,JPMC,CATP,UNIL,LLOY".split(",");

    private ConnectionFactory factory = null;

    public RabbitMqController(RuntimeEnvironment environment) {
        this.environment = environment;
        factory = new ConnectionFactory();
        factory.setHost(environment.getRabbitMqHost());
        factory.setPort(environment.getRabbitMqPort());
    }


    public final String StockSymbolsConfig = "stock.symbols";
//    @Value("${RABBITMQ_HOST}")
//    private String rabbitMqHost;
//
//    @Value("${RABBITMQ_PORT}")
//    private int rabbitMqPort;

    // PUT endpoint (writing messages)
    @PutMapping("/{queueName}/{messageCount}")
    public void putMessages(@PathVariable String queueName, @PathVariable int messageCount) {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(environment.getRabbitMqHost());
        factory.setPort(environment.getRabbitMqPort());

        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {

            channel.queueDeclare(queueName, false, false, false, null);

            for (int i = 0; i < messageCount; i++) {
                String message = String.format("{\"uid\":\"%s\",\"counter\":%d}", STUDENT_UID, i);
                channel.basicPublish("", queueName, null, message.getBytes(StandardCharsets.UTF_8));
                System.out.printf("[x] Sent: %s\n", message);
            }

        } catch (Exception e) {
            throw new RuntimeException("Error while sending messages", e);
        }
    }

    // GET endpoint (reading messages)
    @GetMapping("/{queueName}/{timeoutInMsec}")
    public List<String> getMessages(@PathVariable String queueName, @PathVariable int timeoutInMsec) {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(environment.getRabbitMqHost());
        factory.setPort(environment.getRabbitMqPort());

        List<String> result = new ArrayList<>();

        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {

            channel.queueDeclare(queueName, false, false, false, null);

            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
                System.out.printf("[x] Received: %s\n", message);
                result.add(message);
            };

            String consumerTag = channel.basicConsume(queueName, true, deliverCallback, consumer -> {});
            Thread.sleep(timeoutInMsec);
            channel.basicCancel(consumerTag);  // stop consuming after timeout

        } catch (Exception e) {
            throw new RuntimeException("Error while receiving messages", e);
        }

        return result;
    }


    @PostMapping("/sendJsonObjects/{queueName}")
    public void sendJsonObjects(@PathVariable String queueName, @RequestBody List<Map<String, Object>> messages) {
        logger.info("Sending {} JSON messages to queue {}", messages.size(), queueName);

        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {

            channel.queueDeclare(queueName, false, false, false, null);
            ObjectMapper mapper = new ObjectMapper();

            for (Map<String, Object> message : messages) {
                String json = mapper.writeValueAsString(message);
                channel.basicPublish("", queueName, null, json.getBytes(StandardCharsets.UTF_8));
                System.out.println(" [x] Sent JSON: " + json + " to queue: " + queueName);
            }

            logger.info("Successfully sent all JSON messages to {}", queueName);
        } catch (Exception e) {
            throw new RuntimeException("Failed to send JSON messages to RabbitMQ", e);
        }
    }

    @PostMapping("/sendStockSymbols/{queueName}/{symbolCount}")
    public void sendStockSymbols(@PathVariable String queueName, @PathVariable int symbolCount) {
        logger.info("Writing {} symbols in queue {}", symbolCount, queueName);

        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {

            channel.queueDeclare(queueName, false, false, false, null);

            for (int i = 0; i < symbolCount; i++) {
                final String symbol = stockSymbols[new Random().nextInt(stockSymbols.length)];
                final String value = String.valueOf(i);

                String message = String.format("%s:%s", symbol, value);

                channel.basicPublish("", queueName, null, message.getBytes());
                System.out.println(" [x] Sent message: " + message + " to queue: " + queueName);
            }

            logger.info("{} record(s) sent to Kafka\n", symbolCount);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @GetMapping("/receiveStockSymbols/{queueName}/{consumeTimeMsec}")
    public List<String> receiveStockSymbols(@PathVariable String queueName, @PathVariable int consumeTimeMsec) {
        logger.info(String.format("Reading stock-symbols from queue %s", queueName));
        List<String> result = new ArrayList<>();

        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {


            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
                System.out.printf("[%s]:%s -> %s", queueName, delivery.getEnvelope().getRoutingKey(), message);
                result.add(message);
            };

            System.out.println("start consuming events - to stop press CTRL+C");
            // Consume with Auto-ACK
            channel.basicConsume(queueName, true, deliverCallback, consumerTag -> {});
            Thread.sleep(consumeTimeMsec);

            System.out.printf("done consuming events. %d record(s) received\n", result.size());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return result;
    }
}
