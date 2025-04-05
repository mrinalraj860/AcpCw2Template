package uk.ac.ed.acp.cw2.controller;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.DeliverCallback;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
// Spring Boot annotations and web tools

import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.RestTemplate;

// Kafka libraries

// RabbitMQ libraries
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

// JSON handling
import org.json.JSONObject;

// Java utilities
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

import redis.clients.jedis.Jedis;
import uk.ac.ed.acp.cw2.data.RuntimeEnvironment;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * KafkaController is a REST API controller used to interact with Apache Kafka for producing
 * and consuming stock symbol events. This class provides endpoints for sending stock symbols
 * to a Kafka topic and retrieving stock symbols from a Kafka topic.
 * <p>
 * It is designed to handle dynamic Kafka configurations based on the runtime environment
 * and supports security configurations such as SASL and JAAS.
 */
@RestController()
@RequestMapping("/kafka")
public class KafkaController {

    private static final Logger logger = LoggerFactory.getLogger(KafkaController.class);
    private final RuntimeEnvironment environment;
    private final String[] stockSymbols = "AAPL,MSFT,GOOG,AMZN,TSLA,JPMC,CATP,UNIL,LLOY".split(",");

    public KafkaController(RuntimeEnvironment environment) {
        this.environment = environment;
    }
    private static final String STUDENT_UID = "s2687419";
    /**
     * Constructs Kafka properties required for KafkaProducer and KafkaConsumer configuration.
     *
     * @param environment the runtime environment providing dynamic configuration details
     *                     such as Kafka bootstrap servers.
     * @return a Properties object containing configuration properties for Kafka operations.
     */
    private Properties getKafkaProperties(RuntimeEnvironment environment) {
        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", environment.getKafkaBootstrapServers());
        kafkaProps.put("acks", "all");
        kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaProps.setProperty("enable.auto.commit", "true");
        kafkaProps.put("acks", "all");

        kafkaProps.put("group.id", UUID.randomUUID().toString());
        kafkaProps.setProperty("auto.offset.reset", "earliest");
        kafkaProps.setProperty("enable.auto.commit", "true");

        if (environment.getKafkaSecurityProtocol() != null) {
            kafkaProps.put("security.protocol", environment.getKafkaSecurityProtocol());
        }
        if (environment.getKafkaSaslMechanism() != null) {
            kafkaProps.put("sasl.mechanism", environment.getKafkaSaslMechanism());
        }
        if (environment.getKafkaSaslJaasConfig() != null) {
            kafkaProps.put("sasl.jaas.config", environment.getKafkaSaslJaasConfig());
        }

        return kafkaProps;
    }
    //CW
    // PUT Endpoint: Writing messages to Kafka
    @PutMapping("/{writeTopic}/{messageCount}")
    public String putMessages(@PathVariable String writeTopic, @PathVariable int messageCount) {
        logger.info("Writing {} messages to topic {}", messageCount, writeTopic);
        Properties kafkaProps = getKafkaProperties(environment);

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(kafkaProps)) {
            for (int i = 0; i < messageCount; i++) {
                String message = String.format("{\"uid\":\"%s\",\"counter\":%d}", STUDENT_UID, i);
                producer.send(new ProducerRecord<>(writeTopic, STUDENT_UID, message),
                        (metadata, exception) -> {
                            if (exception != null)
                                logger.error("Error sending message", exception);
                            else
                                logger.info("Produced event to topic {}: offset = {}", writeTopic, metadata.offset());
                        });
            }
        } catch (Exception e) {
            logger.error("Kafka producer error", e);
            throw new RuntimeException("Failed to send Kafka messages", e);
        }

        return "Kafka messages sent successfully!";
    }

    // GET Endpoint: Reading messages from Kafka
    @GetMapping("/{readTopic}/{timeoutInMsec}")
    public List<String> getMessages(@PathVariable String readTopic, @PathVariable int timeoutInMsec) {
        logger.info("Reading messages from topic {}", readTopic);
        Properties kafkaProps = getKafkaProperties(environment);

        List<String> result = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(kafkaProps)) {
            consumer.subscribe(Collections.singletonList(readTopic));

            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(timeoutInMsec));
            for (ConsumerRecord<String, String> record : records) {
                logger.info("[{}] Key: {}, Value: {}, Partition: {}, Offset: {}",
                        record.topic(), record.key(), record.value(), record.partition(), record.offset());
                result.add(record.value());
            }
        } catch (Exception e) {
            logger.error("Kafka consumer error", e);
            throw new RuntimeException("Failed to read Kafka messages", e);
        }

        return result;
    }



    @PostMapping("/publish")
    public ResponseEntity<String> publishToKafka(@RequestBody Map<String, Object> request) {
        String topic = (String) request.get("topic");
        Map<String, Object> message = (Map<String, Object>) request.get("message");

        if (topic == null || message == null) {
            return ResponseEntity.badRequest().body("Missing 'topic' or 'message' field");
        }

        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", environment.getKafkaBootstrapServers());
        kafkaProps.put("acks", "all");
        kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(kafkaProps)) {
            String messageJson = new ObjectMapper().writeValueAsString(message);
            String key = message.get("key") != null ? message.get("key").toString() : UUID.randomUUID().toString();

            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, messageJson);
            producer.send(record);
            return ResponseEntity.ok("Message published to Kafka topic: " + topic);
        } catch (Exception e) {
            e.printStackTrace();
            return ResponseEntity.status(500).body("Kafka publish failed: " + e.getMessage());
        }
    }


    private String getAcpStorageServiceUrl() {
        String defaultUrl = "https://acp-storage.azurewebsites.net";
        return System.getenv("ACP_STORAGE_SERVICE") != null
                ? System.getenv("ACP_STORAGE_SERVICE")
                : defaultUrl;
    }
    private String storeInAcpStorage(String jsonData) {
        String url = getAcpStorageServiceUrl() + "/api/v1/blob";
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<String> request = new HttpEntity<>(jsonData, headers);
        RestTemplate restTemplate =  new RestTemplate();
        ResponseEntity<String> response = restTemplate.postForEntity(url, request, String.class);
        return response.getBody(); // UUID from ACP Storage
    }

    // Corrected RabbitMQ factory creation
    private ConnectionFactory getRabbitMqFactory() {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(environment.getRabbitMqHost()); // fix clearly using your getters
        factory.setPort(environment.getRabbitMqPort());
        return factory;
    }

    @PostMapping("/processMessages")
    public String processMessages(@RequestBody Map<String, Object> request) {
        String readTopic = (String) request.get("readTopic");
        String writeQueueGood = (String) request.get("writeQueueGood");
        String writeQueueBad = (String) request.get("writeQueueBad");
        int messageCount = (int) request.get("messageCount");

        if(messageCount > 500 ){
            messageCount = 500;
        }

        Properties kafkaProps = getKafkaProperties(environment);
        double goodTotal = 0.0;
        double badTotal = 0.0;

        ConnectionFactory factory = getRabbitMqFactory();

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(kafkaProps);
             Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {

            channel.queueDeclare(writeQueueGood, false, false, false, null);
            channel.queueDeclare(writeQueueBad, false, false, false, null);

            consumer.subscribe(Collections.singletonList(readTopic));
            consumer.poll(Duration.ofMillis(500)); // initial poll
            consumer.seekToBeginning(consumer.assignment());

            int processed = 0;
            while (processed < messageCount) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : records) {
                    if (processed >= messageCount) break;
                    processed++;
                    System.out.println(record.topic() + " " + record.partition() + " " + record.offset() + " " + record.value()+ " \n record:" + record);
                    JSONObject json = new JSONObject(record.value());
                    System.out.println(json);
                    String key = json.getString("key");
                    double value = json.getDouble("value");
                    System.out.println("✅ Processed message:\n" + json.toString(2));
                    if ((key.length() == 4) || (key.length() == 3)) { // good message
                        goodTotal += value;
                        json.put("runningTotalValue", goodTotal);

                        String uuid = storeInAcpStorage(json.toString());
                        System.out.println(uuid);
                        json.put("uuid", uuid);

                        channel.basicPublish("", writeQueueGood, null, json.toString().getBytes());
                    } else { // bad message
                        badTotal += value;
                        channel.basicPublish("", writeQueueBad, null, json.toString().getBytes());
                    }
                }
            }

            // Send TOTAL packets
            JSONObject goodTotalJson = new JSONObject();
            goodTotalJson.put("uid", STUDENT_UID);
            goodTotalJson.put("key", "TOTAL");
            goodTotalJson.put("comment", "");
            goodTotalJson.put("value", goodTotal);
            channel.basicPublish("", writeQueueGood, null, goodTotalJson.toString().getBytes());

            JSONObject badTotalJson = new JSONObject();
            badTotalJson.put("uid", STUDENT_UID);
            badTotalJson.put("key", "TOTAL");
            badTotalJson.put("comment", "");
            badTotalJson.put("value", badTotal);
            channel.basicPublish("", writeQueueBad, null, badTotalJson.toString().getBytes());

        } catch (Exception e) {
            logger.error("Error processing messages", e);
            throw new RuntimeException("Failed to process messages", e);
        }

        return "Message processing completed!";
    }

    @PostMapping("/sendStockSymbols/{symbolTopic}/{symbolCount}")
    public void sendStockSymbols(@PathVariable String symbolTopic, @PathVariable int symbolCount) {
        logger.info(String.format("Writing %d symbols in topic %s", symbolCount, symbolTopic));
        Properties kafkaProps = getKafkaProperties(environment);

        try (var producer = new KafkaProducer<String, String>(kafkaProps)) {
            for (int i = 0; i < symbolCount; i++) {
                final String key = stockSymbols[new Random().nextInt(stockSymbols.length)];
                final String value = String.valueOf(i);

                producer.send(new ProducerRecord<>(symbolTopic, key, value), (recordMetadata, ex) -> {
                    if (ex != null)
                        ex.printStackTrace();
                    else
                        logger.info(String.format("Produced event to topic %s: key = %-10s value = %s%n", symbolTopic, key, value));
                }).get(1000, TimeUnit.MILLISECONDS);
            }
            logger.info(String.format("%d record(s) sent to Kafka\n", symbolCount));
        } catch (ExecutionException e) {
            logger.error("execution exc: " + e);
            throw new RuntimeException(e);
        } catch (TimeoutException e) {
            logger.error("timeout exc: " + e);
        } catch (InterruptedException e) {
            logger.error("interrupted exc: " + e);
            throw new RuntimeException(e);
        }
    }

    @GetMapping("/receiveStockSymbols/{symbolTopic}/{consumeTimeMsec}")
    public List<AbstractMap.SimpleEntry<String, String>> receiveStockSymbols(@PathVariable String symbolTopic, @PathVariable int consumeTimeMsec) {
        logger.info(String.format("Reading stock-symbols from topic %s", symbolTopic));
        Properties kafkaProps = getKafkaProperties(environment);

        var result = new ArrayList<AbstractMap.SimpleEntry<String, String>>();

        try (var consumer = new KafkaConsumer<String, String>(kafkaProps)) {
            consumer.subscribe(Collections.singletonList(symbolTopic));
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(consumeTimeMsec));
            for (ConsumerRecord<String, String> record : records) {
                logger.info(String.format("[%s] %s: %s %s %s %s", record.topic(), record.key(), record.value(), record.partition(), record.offset(), record.timestamp()));
                result.add(new AbstractMap.SimpleEntry<>(record.key(), record.value()));
            }
        }

        return result;
    }


    @PostMapping("/transformMessages")
    public String transformMessages(@RequestBody Map<String, Object> request) {
        String readQueue = (String) request.get("readQueue");
        String writeQueue = (String) request.get("writeQueue");
        int messageCount = (int) request.get("messageCount");

        // Redis setup
        Jedis jedis = new Jedis(environment.getRedisHost(), environment.getRedisPort());

        // RabbitMQ setup
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(environment.getRabbitMqHost());
        factory.setPort(environment.getRabbitMqPort());

        AtomicInteger totalMessagesProcessed = new AtomicInteger(0);
        AtomicInteger totalMessagesWritten = new AtomicInteger(0);
        AtomicInteger totalRedisUpdates = new AtomicInteger(0);
        AtomicReference<Double> totalValueWritten = new AtomicReference<>(0.0);
        AtomicReference<Double> totalAdded = new AtomicReference<>(0.0);


        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {

            channel.queueDeclare(readQueue, false, false, false, null);
            channel.queueDeclare(writeQueue, false, false, false, null);

            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                String msgBody = new String(delivery.getBody(), StandardCharsets.UTF_8);
                JSONObject message = new JSONObject(msgBody);
                totalMessagesProcessed.getAndIncrement();

                if (message.has("version") && message.has("value")) {
                    // normal message
                    String key = message.getString("key");
                    int version = message.getInt("version");
                    double value = message.getDouble("value");

                    String redisKey = "version:" + key;
                    int storedVersion = jedis.exists(redisKey) ? Integer.parseInt(jedis.get(redisKey)) : -1;

                    if (storedVersion < version) {
                        channel.basicPublish("", writeQueue, null, msgBody.getBytes(StandardCharsets.UTF_8));
                        totalMessagesWritten.incrementAndGet();
                        totalValueWritten.set(totalValueWritten.get() + value);
                    } else {
                        jedis.set(redisKey, String.valueOf(version));
                        message.put("value", value + 10.5);

                        totalMessagesWritten.incrementAndGet();
                        totalRedisUpdates.incrementAndGet();
                        totalAdded.set(totalAdded.get() + 10.5);
                        totalValueWritten.set(totalValueWritten.get() + value + 10.5);

                        channel.basicPublish("", writeQueue, null, message.toString().getBytes(StandardCharsets.UTF_8));
                    }

                } else if (message.has("key")) {
                    // tombstone message
                    String key = message.getString("key");
                    jedis.del("version:" + key);

                    JSONObject summary = new JSONObject();
                    summary.put("totalMessagesWritten", totalMessagesWritten.get());
                    summary.put("totalMessagesProcessed", totalMessagesProcessed.get());
                    summary.put("totalRedisUpdates", totalRedisUpdates.get());
                    summary.put("totalValueWritten", totalValueWritten.get());
                    summary.put("totalAdded", totalAdded.get());

                    channel.basicPublish("", writeQueue, null, summary.toString().getBytes(StandardCharsets.UTF_8));
                }

            };

            // Start consuming and wait until messageCount is reached
            String consumerTag = channel.basicConsume(readQueue, true, deliverCallback, consumerTag1 -> {});
            while (totalMessagesProcessed.get() < messageCount) {
                Thread.sleep(100); // allow time for messages
            }
            channel.basicCancel(consumerTag);
        } catch (Exception e) {
            throw new RuntimeException("Error processing transformMessages", e);
        } finally {
            jedis.close();
        }

        return "Transform processing complete!";
    }
}
