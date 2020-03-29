package com.sha.springbootkafka.listener;

import com.sha.springbootkafka.model.MessageEntity;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.PartitionOffset;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

/**
 * @author sa
 * @date 2020-03-28
 * @time 12:20
 */
@Component
public class MessageListener
{
    //Multiple listeners can be implemented for a topic, each with a different group id.
    //Furthermore, one consumer can listen for messages from various topics.
    //@KafkaListener(topics = "${spring.kafka.template.first-topic}", containerFactory = "firstKafkaListenerContainerFactory")
    public void listenFirstTopic(Object message) //This is basic form.
    {
        System.out.println("Received message in group groupId1: " + message);
    }

    //This is detail form.
    //@KafkaListener(topics = "${spring.kafka.template.first-topic}", containerFactory = "firstKafkaListenerContainerFactory")
    public void listenFirstTopicWithDetails(ConsumerRecord<String, MessageEntity> consumerRecord,
                                            @Payload MessageEntity messageEntity,
                                            @Header(KafkaHeaders.GROUP_ID) String groupId,
                                            @Header(KafkaHeaders.OFFSET) int offset,
                                            @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition)
    {
        System.out.println("Received message with below details:");
        System.out.println(consumerRecord); //We can also reach to all details from consumerRecord.
        System.out.println(messageEntity); //This is value.
        System.out.println(groupId); //This will be groupId1.
        System.out.println(offset); //Current record offset.
        System.out.println(partition); //We used only one partition, so this will be 0 for each message.
    }

    @KafkaListener(containerFactory = "firstKafkaListenerContainerFactory", topicPartitions = {
            @TopicPartition(topic = "${spring.kafka.template.first-topic}",
                    partitionOffsets = @PartitionOffset(partition = "0", initialOffset = "0"))
    })
    //This provides to fetch messages from beginning for each time when application is up.
    //When we publish new message, it will go on with current offset. not from beginning.
    public void listenFirstTopicFromBeginning(ConsumerRecord<String, MessageEntity> consumerRecord)
    {
        System.out.println(consumerRecord.value() + " with offset : " + consumerRecord.offset());
    }

    @KafkaListener(groupId = "groupPartition", topics = "${spring.kafka.template.partition-topic}")
    public void listenSecondTopic(Object message, @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition)
    {
        System.out.println("----------------");
        System.out.println("Received message with below details : ");
        System.out.println(message);
        System.out.println("partition : " + partition);
        System.out.println("----------------");
    }

    //Filter key = "test"
    @KafkaListener(topics = "${spring.kafka.template.partition-topic}", containerFactory = "filterKafkaListenerContainerFactory")
    public void filterListener(Object message)
    {
        System.out.println("----------------");
        System.out.println("Received message with filter with below details : ");
        System.out.println(message);
        System.out.println("----------------");
    }

    //By the way, we will also see in here same topic (partition-topic) can be listen by multiple groupConsumer (groupPartition, groupFilter, groupHandler)
    //But if we try to listen a topic with same groupConsumer from different listeners, we can reach only one of them.
    @KafkaListener(topics = "${spring.kafka.template.partition-topic}", containerFactory = "handlerKafkaListenerContainerFactory")
    public void errorHandlerListener(Object message, @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) String key)
    {
        System.out.println("----------------");
        System.out.println("Received message with error handler with below details : ");
        System.out.println(message);
        System.out.println("----------------");
        //To test it, we will send messages with "error" key.
        if ("error".equals(key))
        {
            throw new RuntimeException();
        }
    }

    //If there is an error on producer, none of messages in transaction will be reached to listener.
    @KafkaListener(topics = "${spring.kafka.template.transactional-topic}", containerFactory = "transactionalKafkaListenerContainerFactory")
    public void transactionalListener(Object message)
    {
        System.out.println("----------------");
        System.out.println("Received message with transactional with below details : ");
        System.out.println(message);
        System.out.println("----------------");
    }
}
