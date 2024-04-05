package com.cy.base.producer;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

/**
 * 同步消息
 *
 * @author CY
 * @description
 * @date 2024/04/05
 */
public class SyncProducer {
    public static void main(String[] args) throws Exception{
        // 1.创建消息生产者producer，并制定生产者组名
        DefaultMQProducer mqProducer = new DefaultMQProducer("sync");
        // 2.指定Nameserver地址
        mqProducer.setNamesrvAddr("192.168.123.102:9876;192.168.123.108:9876");
        // 3.启动producer
        mqProducer.start();
        // 4.创建消息对象，指定主题Topic、Tag和消息体
        for (int i = 1; i <= 10; i++) {
            Message message = new Message("sync_topic", "sync_tag", ("hello mq1 " + i).getBytes());
            // 5.发送消息
            SendResult sendResult = mqProducer.send(message);
            System.out.println(sendResult);
        }
        // 6.关闭生产者producer
        mqProducer.shutdown();
    }
}
