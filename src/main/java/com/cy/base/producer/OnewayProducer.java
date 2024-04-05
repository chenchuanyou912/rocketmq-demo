package com.cy.base.producer;

import com.alibaba.fastjson2.JSON;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

import java.util.concurrent.TimeUnit;

public class OnewayProducer {

    public static void main(String[] args) throws Exception {
        // 1.创建消息生产者producer，并制定生产者组名
        DefaultMQProducer mqProducer = new DefaultMQProducer("oneway");
        // 2.指定Nameserver地址
        mqProducer.setNamesrvAddr("192.168.123.102:9876;192.168.123.108:9876");
        // 3.启动producer
        mqProducer.start();
        // 4.创建消息对象，指定主题Topic、Tag和消息体
        for (int i = 1; i <= 10; i++) {
            Message message = new Message("oneway_topic", "oneway_tag", ("hello mq " + i).getBytes());
            // 5.发送消息
            mqProducer.sendOneway(message);
        }
        // 6.关闭生产者producer
        mqProducer.shutdown();
    }
}
