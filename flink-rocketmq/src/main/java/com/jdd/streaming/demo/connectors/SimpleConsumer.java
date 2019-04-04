package com.jdd.streaming.demo.connectors;

import org.apache.commons.compress.utils.ByteUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.consumer.*;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static com.jdd.streaming.demo.connectors.common.RocketMQUtils.getInteger;

/**
 * @Auther: dalan
 * @Date: 19-4-4 09:28
 * @Description:
 */
public class SimpleConsumer {
    public static void main(String[] args) throws InterruptedException, MQClientException {
        //directDefaultConsumer();
        pullServiceConsumer();
    }

    public static void pullServiceConsumer() throws MQClientException {
        MQPullConsumerScheduleService pull = null;
        final DefaultMQPullConsumer consumer;


        pull = new MQPullConsumerScheduleService("TestGroup");
        consumer = pull.getDefaultMQPullConsumer();


        // Specify name server addresses.
        consumer.setNamesrvAddr("localhost:9876");
        consumer.setPollNameServerInterval(30000);
        consumer.setHeartbeatBrokerInterval(30000);
        consumer.setMessageModel(MessageModel.CLUSTERING);
        consumer.setPersistConsumerOffsetInterval(5000);
        // Subscribe one more more topics to consume.
        //consumer.fetchSubscribeMessageQueues("TopicTest");

        pull.setPullThreadNums(2);
        pull.registerPullTaskCallback("TopicTest1", new PullTaskCallback() {
            @Override public void doPullTask(MessageQueue mq, PullTaskContext pullTaskContext) {
                long offset = 0;
                try {
                    offset = consumer.fetchConsumeOffset(mq, false);
                } catch (MQClientException e) {
                    e.printStackTrace();
                }

                if (offset < 0) {
                    return;
                }

                try {
                    PullResult pullResult = consumer.pull(mq, "TagB", offset, 2);

                    List<MessageExt> messages = pullResult.getMsgFoundList();
                    for (MessageExt msg : messages) {
                        //System.out.println(msg);

                        byte[] key = msg.getKeys() != null ? msg.getKeys().getBytes(StandardCharsets.UTF_8) : null;
                        byte[] value = msg.getBody();

                        System.out.printf("the message keys = %s, and body %s\n",
                            new String(key,StandardCharsets.UTF_8),
                            new String(value,StandardCharsets.UTF_8));
                    }

                } catch (MQClientException e) {
                    e.printStackTrace();
                } catch (RemotingException e) {
                    e.printStackTrace();
                } catch (MQBrokerException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (Exception e){
                    e.printStackTrace();
                }
            }
        });

        pull.start();
        //consumer.start();
        System.out.printf("Consumer Started.%n");
    }

    public static void directDefaultConsumer() throws MQClientException {
        // Instantiate with specified consumer group name.
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("please_rename_unique_group_name");

        // Specify name server addresses.
        consumer.setNamesrvAddr("localhost:9876");
        consumer.setConsumerGroup("TestGroup");

        // Subscribe one more more topics to consume.
        consumer.subscribe("TopicTest", "*");


        // Register callback to execute on arrival of messages fetched from brokers.
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
                ConsumeConcurrentlyContext context) {
                for (MessageExt msg : msgs) {
                    try {
                        System.out.printf("the message keys = %s, and body %s\n",
                            new String(msg.getProperties().get("KEYS").getBytes("UTF-8"),"UTF-8"),
                            new String(msg.getBody(),"UTF-8")
                        );
                    } catch (UnsupportedEncodingException e) {
                        e.printStackTrace();
                    }
                }
                System.out.printf("%s Receive New Messages: %s %n", Thread.currentThread().getName(), msgs);
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        //Launch the consumer instance.
        consumer.start();

        System.out.printf("Consumer Started.%n");
    }
}
