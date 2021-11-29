package com.example.rocketmq.simple;

import com.example.rocketmq.RocketConfig;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;

public class Consumer {
    private static final Logger LOGGER = LoggerFactory.getLogger(Consumer.class);

    public static void main(String[] args) throws MQClientException {

        // Instantiate with specified consumer group name.
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("Jodie_Daily_test");

        // Specify name server addresses.
        consumer.setNamesrvAddr(RocketConfig.getNameServer());

        // Subscribe one more more topics to consume.
        consumer.subscribe("Jodie_topic_1023", "*");
        // Register callback to execute on arrival of messages fetched from brokers.
        consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
            for (MessageExt msg : msgs) {
                LOGGER.info("Receive New Messages, broker:{},queue:{},topic:{},content:{}",
                        context.getMessageQueue().getBrokerName(), context.getMessageQueue().getQueueId(),
                        msg.getTopic(), new String(msg.getBody(), StandardCharsets.UTF_8));
            }
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });

        //Launch the consumer instance.
        consumer.start();

        LOGGER.info("Consumer Started.");
    }
}
