package com.example.poc_kafka_consumer.kafka;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.reactive.ReactiveKafkaConsumerTemplate;
import reactor.kafka.receiver.ReceiverOptions;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;


@EnableKafka
@Configuration
@Slf4j
@RequiredArgsConstructor
public class KafkaConfig {

  @Value("${spring.kafka.bootstrap-servers}")
  private String bootstrapAddress;

  @Bean
  public ReceiverOptions<String, String> kafkaReceiverOptions(
      @Value(value = "${spring.kafka.topics.raw-input-data}") String topic) {
    Map<String, Object> props = new HashMap<>();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, Boolean.FALSE.toString());
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "bcp-poc-group-id");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG, 5000);

    ReceiverOptions<String, String> basicReceiverOptions = ReceiverOptions.create(props);
    return basicReceiverOptions.subscription(Collections.singletonList(topic));
  }

  @Bean
  public ReactiveKafkaConsumerTemplate<String, String> reactiveKafkaConsumerTemplate(
      ReceiverOptions<String, String> kafkaReceiverOptions) {
    return new ReactiveKafkaConsumerTemplate<>(kafkaReceiverOptions);
  }

  @Bean
  public KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry() {
    return new KafkaListenerEndpointRegistry();
  }
}
