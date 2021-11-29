package com.example.rocketmq.simple;

import com.example.rocketmq.RocketConfig;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * 异步发送，异步传输通常用于响应时间敏感的业务场景
 */
public class AsyncProducer {
    private static final Logger LOGGER = LoggerFactory.getLogger(AsyncProducer.class);

    public static void main(String[] args) throws Exception {
        //Instantiate with a producer group name.
        DefaultMQProducer producer = new DefaultMQProducer("Jodie_Daily_test");
        // Specify name server addresses.
        producer.setNamesrvAddr(RocketConfig.getNameServer());
        //Launch the instance.
        producer.start();
        producer.setSendMsgTimeout(15000);
        producer.setRetryTimesWhenSendAsyncFailed(0);
        producer.setVipChannelEnabled(false);
        int messageCount = 100;
        final CountDownLatch countDownLatch = new CountDownLatch(messageCount);
        for (int i = 0; i < messageCount; i++) {
            try {
                final int index = i;
                Message msg = new Message("Jodie_topic_1023",
                        "TagA",
                        "OrderID188",
                        "Hello world".getBytes(StandardCharsets.UTF_8));
                producer.send(msg, new SendCallback() {
                    @Override
                    public void onSuccess(SendResult sendResult) {
                        countDownLatch.countDown();
                        LOGGER.info("OK {} {}", index, sendResult.getMsgId());
                    }

                    @Override
                    public void onException(Throwable e) {
                        countDownLatch.countDown();
                        LOGGER.error(e.getMessage(), e);
                    }
                });
            } catch (InterruptedException e) {
                LOGGER.error(e.getMessage(), e);
                Thread.currentThread().interrupt();
            }
        }
        // 官方示例这里有坑，需要时间设置长一些
        countDownLatch.await(15, TimeUnit.SECONDS);
        producer.shutdown();
    }
}
