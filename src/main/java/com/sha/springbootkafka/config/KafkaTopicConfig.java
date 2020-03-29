package com.sha.springbootkafka.config;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;

import java.util.HashMap;
import java.util.Map;

/**
 * @author sa
 * @date 2020-03-26
 * @time 23:10
 */
@Configuration
public class KafkaTopicConfig
{
    @Value(value = "${spring.kafka.consumer.bootstrap-servers}")
    private String bootstrapAddress;

    @Value(value = "${spring.kafka.template.first-topic}")
    private String firstTopicName;

    @Value(value = "${spring.kafka.template.partition-topic}")
    private String secondTopicName;

    @Value(value = "${spring.kafka.template.transactional-topic}")
    private String thirdTopicName;

    //We need to add it, which will automatically add topics for all beans of type NewTopic.
    @Bean
    public KafkaAdmin kafkaAdmin()
    {
        //Customize Admin configurations according to your desire with key-value mapping.
        Map<String, Object> configs = new HashMap<>();
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
        return new KafkaAdmin(configs);
    }

    @Bean
    public NewTopic firstTopic()
    {
        return TopicBuilder.name(firstTopicName)
                .partitions(1) // a topic partition is the unit of parallelism in the Kafka.
                .replicas(1) // A partition can have multiple replicas, each stored on a different broker.
                //RETENTION_MS_CONFIG; Messages will be stored in this ms. After this ms, automatically will be removed.
                .config(TopicConfig.RETENTION_MS_CONFIG, "100000000") //We can customize the default configs.
                .build();
    }

    @Bean
    public NewTopic secondTopic()
    {
        return TopicBuilder.name(secondTopicName).partitions(3).replicas(2).build();
    }

    @Bean
    public NewTopic thirdTopic()
    {
        return TopicBuilder.name(thirdTopicName).partitions(2).replicas(1).build();
    }
}
