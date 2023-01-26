/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.client.impl.consumer;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.*;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class DefaultMQPushConsumerImplTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void checkConfigTest() throws MQClientException {

        //test type
        thrown.expect(MQClientException.class);

        //test message
        thrown.expectMessage("consumeThreadMin (10) is larger than consumeThreadMax (9)");

        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("test_consumer_group");

        consumer.setConsumeThreadMin(9);
        consumer.setConsumeThreadMax(10);

        consumer.registerMessageListener(new MessageListenerConcurrently() {
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
                ConsumeConcurrentlyContext context) {
                System.out.println(" Receive New Messages: " + msgs);
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        DefaultMQPushConsumerImpl defaultMQPushConsumerImpl = new DefaultMQPushConsumerImpl(consumer, null);
        defaultMQPushConsumerImpl.start();
    }

    @Test
    public void test_consume() throws MQClientException, InterruptedException {

        String namesrvAddr = "127.0.0.1:9876";
        String group = "test_consumer_group";
        String topic = "test_hello_rocketmq";
        // 初始化consumer
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer();
        consumer.setNamesrvAddr(namesrvAddr);
        consumer.setConsumerGroup(group);
        // 订阅topic
        consumer.subscribe(topic, (String) null);
        // 设置消费的位置，由于producer已经发送了消息，所以我们设置从第一个开始消费
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        // 添加消息监听器
        consumer.registerMessageListener(new MessageListenerOrderly() {
            @Override
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
                msgs.forEach(msg -> {
                    System.out.println(new String(msg.getBody()));
                });
                return ConsumeOrderlyStatus.SUCCESS;
            }
        });
        // 启动consumer
        consumer.start();
        // 由于是异步消费，所以不能立即关闭，防止消息还未消费到
        TimeUnit.SECONDS.sleep(2);
        consumer.shutdown();
    }
}
