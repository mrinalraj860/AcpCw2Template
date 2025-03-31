package uk.ac.ed.acp.cw2.controller;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StopWatch;
import org.springframework.web.bind.annotation.*;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import uk.ac.ed.acp.cw2.data.RuntimeEnvironment;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * KafkaController is a REST API controller used to interact with Apache Kafka for producing
 * and consuming stock symbol events. This class provides endpoints for sending stock symbols
 * to a Kafka topic and retrieving stock symbols from a Kafka topic.
 * <p>
 * It is designed to handle dynamic Kafka configurations based on the runtime environment
 * and supports security configurations such as SASL and JAAS.
 */
@RestController()
@RequestMapping("/api/v1/kafka")
public class KafkaController {

    private static final Logger logger = LoggerFactory.getLogger(KafkaController.class);
    private final RuntimeEnvironment environment;
    private final String[] stockSymbols = "AAPL,MSFT,GOOG,AMZN,TSLA,JPMC,CATP,UNIL,LLOY".split(",");

    public KafkaController(RuntimeEnvironment environment) {
        this.environment = environment;
    }

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
}
