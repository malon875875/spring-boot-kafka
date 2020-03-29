package com.sha.springbootkafka.config;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.SeekToCurrentErrorHandler;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * @author sa
 * @date 2020-03-28
 * @time 12:07
 */
@EnableKafka // it is required on the configuration class to enable detection of @KafkaListener.
@Configuration
public class ConsumerTopicConfig
{
    @Value(value = "${spring.kafka.consumer.bootstrap-servers}")
    private String bootstrapAddress;

    public ConsumerFactory<String, Object> consumerFactory(String groupId)
    {
        return consumerFactory(groupId, null);
    }

    //<Key,Value> pair should match with producer factory.
    //groupId -> consumerGroupId
    public ConsumerFactory<String, Object> consumerFactory(String groupId, String isolationLevel)
    {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
        //Different consumer groups can share the same record or same consumer-group can share the work.
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        //It has same type with producer key.
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        //In default, consumer does not wait to producer-done-commit-state. -> unread_committed.
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, Objects.requireNonNullElse(isolationLevel, ConsumerConfig.DEFAULT_ISOLATION_LEVEL));

        JsonDeserializer<Object> jsonDeserializer = new JsonDeserializer<>();
        //Producer records can be any class under this package.
        jsonDeserializer.addTrustedPackages("com.sha.springbootkafka.model");
        return new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(), jsonDeserializer);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> firstKafkaListenerContainerFactory()
    {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory("groupId1"));
        factory.setConcurrency(3);
        factory.setAutoStartup(true); //It will be automatically started when application is up.
        return factory;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> filterKafkaListenerContainerFactory()
    {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory("groupFilter"));
        //If key is "test", then remove it so message listener couldn't reach it.
        factory.setRecordFilterStrategy(consumerRecord -> "test".equals(consumerRecord.key()));
        factory.setConcurrency(3);
        factory.setAutoStartup(true);
        return factory;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> handlerKafkaListenerContainerFactory()
    {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory("groupHandler"));
        //If there is an error on listener, then we will handle it retry it 3 times with 500ms interval.
        factory.setErrorHandler(new SeekToCurrentErrorHandler(new FixedBackOff(500, 3)));
        factory.setConcurrency(3);
        factory.setAutoStartup(true);
        return factory;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> transactionalKafkaListenerContainerFactory()
    {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        //read_committed -> provides transactional.
        factory.setConsumerFactory(consumerFactory("groupTransactional", "read_committed"));
        factory.setConcurrency(3);
        factory.setAutoStartup(true);
        return factory;
    }

    @Bean
    public Consumer<String, Object> manualConsumer()
    {
        return consumerFactory("groupManual").createConsumer();
    }
}
