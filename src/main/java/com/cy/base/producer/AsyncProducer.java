package com.cy.base.producer;

import com.alibaba.fastjson2.JSON;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

import java.util.concurrent.TimeUnit;

/**
 * 异步消息
 *
 * @author CY
 * @description
 * @date 2024/04/05
 */
@Slf4j
public class AsyncProducer {
    public static void main(String[] args) throws Exception{
        // 1.创建消息生产者producer，并制定生产者组名
        DefaultMQProducer mqProducer = new DefaultMQProducer("sync");
        // 2.指定Nameserver地址
        mqProducer.setNamesrvAddr("192.168.123.102:9876;192.168.123.108:9876");
        // 3.启动producer
        mqProducer.start();
        // 4.创建消息对象，指定主题Topic、Tag和消息体
        for (int i = 1; i <= 10; i++) {
            Message message = new Message("async_topic", "async_tag", ("hello mq " + i).getBytes());
            // 5.发送消息
            mqProducer.send(message, new SendCallback() {
                @Override
                public void onSuccess(SendResult sendResult) {
                    System.out.println(sendResult);
                }

                @Override
                public void onException(Throwable throwable) {
                    log.error("发送异步消息发生异常：{}", JSON.toJSONString(throwable));
                }
            });
            TimeUnit.SECONDS.sleep(1);
        }
        // 6.关闭生产者producer
        mqProducer.shutdown();
    }
}
