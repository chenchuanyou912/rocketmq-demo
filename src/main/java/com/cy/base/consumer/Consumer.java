package com.cy.base.consumer;

import com.alibaba.fastjson2.JSON;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListener;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.protocol.heartbeat.MessageModel;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class Consumer {
    public static void main(String[] args) throws Exception{
        // 1.创建消费者Consumer，制定消费者组名
        DefaultMQPushConsumer consumer1 = new DefaultMQPushConsumer("consumer1");
        // 2.指定Nameserver地址
        consumer1.setNamesrvAddr("192.168.123.102:9876;192.168.123.108:9876");
        // 3.订阅主题Topic和Tag
        consumer1.subscribe("sync_topic","sync_tag");
//        consumer1.subscribe("sync_topic","*");
        consumer1.setMessageModel(MessageModel.BROADCASTING);
        // 4.设置回调函数，处理消息
        consumer1.registerMessageListener(new MessageListenerConcurrently(){
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext consumeConcurrentlyContext) {
                for (MessageExt msg : list) {
                    System.out.println(new String(msg.getBody()));
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        // 5.启动消费者consumer
        consumer1.start();

    }
}
