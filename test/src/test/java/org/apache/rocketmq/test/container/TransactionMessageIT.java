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

package org.apache.rocketmq.test.container;

import java.io.UnsupportedEncodingException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.client.producer.TransactionSendResult;
import org.apache.rocketmq.common.BrokerIdentity;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

public class TransactionMessageIT extends ContainerIntegrationTestBase {

    private static final String CONSUME_GROUP = TransactionMessageIT.class.getSimpleName() + "_Consumer";
    private static final String PRODUCER_GROUP = TransactionMessageIT.class.getSimpleName() + "_PRODUCER";
    private static final String MESSAGE_STRING = RandomStringUtils.random(1024);
    private static byte[] MESSAGE_BODY;

    static {
        try {
            MESSAGE_BODY = MESSAGE_STRING.getBytes(RemotingHelper.DEFAULT_CHARSET);
        } catch (UnsupportedEncodingException ignored) {
        }
    }

    private static final String TOPIC = TransactionMessageIT.class.getSimpleName() + "_TOPIC";
    private static final int MESSAGE_COUNT = 16;

    public TransactionMessageIT() {
    }

    @BeforeClass
    public static void beforeClass() throws Throwable {
        createTopicTo(master1With3Replicas, TOPIC, 1, 1);
//        createTopicTo(master2With3Replicas, TOPIC, 1, 1);
    }

    @AfterClass
    public static void afterClass() throws Exception {
    }


    @Test
    public void consumeTransactionMsg() throws MQClientException {
        DefaultMQPushConsumer pushConsumer = createPushConsumer(CONSUME_GROUP);
        pushConsumer.subscribe(TOPIC, "*");
        AtomicInteger receivedMsgCount = new AtomicInteger(0);
        pushConsumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
            receivedMsgCount.addAndGet(msgs.size());
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        pushConsumer.start();

        TransactionMQProducer producer = createTransactionProducer(PRODUCER_GROUP, new TransactionListenerImpl(false));
        producer.start();

        for (int i = 0; i < MESSAGE_COUNT; i++) {
            Message msg = new Message(TOPIC, MESSAGE_BODY);
            TransactionSendResult result = producer.sendMessageInTransaction(msg, null);//.sendMessageInTransaction(msg, new TransactionExecutorImpl(), null);
            assertThat(result.getLocalTransactionState()).isEqualTo(LocalTransactionState.COMMIT_MESSAGE);
        }

        System.out.printf("send message complete%n");

        await().atMost(Duration.ofSeconds(MESSAGE_COUNT * 2)).until(() -> receivedMsgCount.get() >= MESSAGE_COUNT);

        System.out.printf("consumer received %d msg%n", receivedMsgCount.get());

        pushConsumer.shutdown();
        producer.shutdown();
    }


    @Test
    public void consumeTransactionMsgFromSlave() throws Exception {
        DefaultMQPushConsumer pushConsumer = createPushConsumer(CONSUME_GROUP);
        pushConsumer.subscribe(TOPIC, "*");
        AtomicInteger receivedMsgCount = new AtomicInteger(0);
        Map<String, Message> msgSentMap = new HashMap<>();
        pushConsumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
            for (MessageExt msg : msgs) {
                System.out.println("receive trans msgId=" + msg.getMsgId() + ", transactionId=" + msg.getTransactionId());
                if (msgSentMap.containsKey(msg.getMsgId())) {
                    receivedMsgCount.incrementAndGet();
                }
            }

            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        pushConsumer.start();

        List<SendResult> results = new ArrayList<>();


        TransactionListenerImpl transactionCheckListener = new TransactionListenerImpl(true);
        TransactionMQProducer producer = createTransactionProducer(PRODUCER_GROUP, transactionCheckListener);
        producer.start();

        for (int i = 0; i < MESSAGE_COUNT; i++) {
            Message msg = new Message(TOPIC, MESSAGE_BODY);
            msg.setKeys(UUID.randomUUID().toString());
            MessageAccessor.putProperty(msg, MessageConst.PROPERTY_TRANSACTION_PREPARED, "true");
            MessageAccessor.putProperty(msg, MessageConst.PROPERTY_PRODUCER_GROUP, PRODUCER_GROUP);
            SendResult result = producer.sendMessageInTransaction(msg, null);
            String msgId = result.getMsgId();
            System.out.println("Sent trans msgid=" + msgId + ", transactionId=" + result.getTransactionId() + ", key=" + msg.getKeys());
            results.add(result);

            msgSentMap.put(msgId, msg);
        }

        isolateBroker(master1With3Replicas);
        brokerContainer1.removeBroker(new BrokerIdentity(master1With3Replicas.getBrokerIdentity().getBrokerClusterName(),
            master1With3Replicas.getBrokerIdentity().getBrokerName(),
            master1With3Replicas.getBrokerIdentity().getBrokerId()));
        createTopicTo(master2With3Replicas, TOPIC, 1, 1);

        transactionCheckListener.setShouldCommitUnknownState(false);
        producer.getDefaultMQProducerImpl().getmQClientFactory().updateTopicRouteInfoFromNameServer(TOPIC);

        System.out.printf("Wait for consuming%n");

        await().atMost(Duration.ofSeconds(300)).until(() -> receivedMsgCount.get() >= MESSAGE_COUNT);

        System.out.printf("consumer received %d msg%n", receivedMsgCount.get());

        pushConsumer.shutdown();
        producer.shutdown();

        master1With3Replicas = brokerContainer1.addBroker(master1With3Replicas.getBrokerConfig(), master1With3Replicas.getMessageStoreConfig());
        master1With3Replicas.start();
        cancelIsolatedBroker(master1With3Replicas);

        awaitUntilSlaveOK();

        receivedMsgCount.set(0);
        DefaultMQPushConsumer pushConsumer2 = createPushConsumer(CONSUME_GROUP);
        pushConsumer2.subscribe(TOPIC, "*");
        pushConsumer2.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
            for (MessageExt msg : msgs) {
                System.out.println("[After master recovered] receive trans msgId=" + msg.getMsgId() + ", transactionId=" + msg.getTransactionId());
                if (msgSentMap.containsKey(msg.getMsgId())) {
                    receivedMsgCount.incrementAndGet();
                }
            }

            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        pushConsumer2.start();
        System.out.println("Wait for checking...");
        Thread.sleep(10000L);
        assertThat(receivedMsgCount.get()).isEqualTo(0);

    }
}
