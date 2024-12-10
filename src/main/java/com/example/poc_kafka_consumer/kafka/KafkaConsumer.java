package com.example.poc_kafka_consumer.kafka;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.boot.context.event.ApplicationStartedEvent;
import org.springframework.context.annotation.Bean;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.core.reactive.ReactiveKafkaConsumerTemplate;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import java.time.Duration;



@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaConsumer {

    private final ReactiveKafkaConsumerTemplate<String, String> reactiveKafkaConsumerTemplate;

    @EventListener(ApplicationStartedEvent.class)
    public Flux<String> consumeKafkaMessages() {
        return reactiveKafkaConsumerTemplate
                .receiveAutoAck()
                .transform(this::processMessages)
                .doOnNext(record -> log.info("Processed message: {}", record))
                .onErrorContinue((ex, obj) -> log.error("Error processing message", ex));
    }

    private Flux<String> processMessages(Flux<ConsumerRecord<String, String>> flux) {
        return flux.transform(this::limitAndPause)
                .map(ConsumerRecord::value);
    }

    private Flux<ConsumerRecord<String, String>> limitAndPause(Flux<ConsumerRecord<String, String>> flux) {
        return flux.index()
                .flatMap(indexedRecord -> {
                    ConsumerRecord<String, String> record = indexedRecord.getT2();
                    long index = indexedRecord.getT1();

                    if (index >= 7) {
                        log.info("Reached message limit, pausing for 20 seconds");
                        TopicPartition topicPartition = new TopicPartition(record.topic(), record.partition());
                        return reactiveKafkaConsumerTemplate.pause(topicPartition)
                                .then(Mono.delay(Duration.ofSeconds(5)))
                                .then(reactiveKafkaConsumerTemplate.resume(topicPartition))
                                .thenReturn(record);
                    }
                    return Mono.just(record);
                });
    }
}