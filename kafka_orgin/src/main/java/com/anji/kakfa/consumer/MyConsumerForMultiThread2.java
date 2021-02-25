package com.anji.kakfa.consumer;

import com.anji.kakfa.utils.ConfigUtils;
import com.anji.kakfa.utils.enums.PorpertiesLoggerLevel;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * \* Date: 2021/2/24
 * \* Author: XiaomingZhang
 * \* Desc:用一个到多个消费者拉取消息,使用多线程处理这些消息,可以实现动态提高大于分区数的数据处理能力,并且减少tcp连接
 */
public class MyConsumerForMultiThread2 {

    public static void main(String[] args) {
        MainThread mainThread = new MainThread(1, 4);
        mainThread.run();
    }

    public static class MainThread implements Runnable {
        protected Integer consumerNum;
        protected Integer processNum;

        public MainThread(Integer consumerNum, Integer processNum) {
            this.consumerNum = consumerNum;
            this.processNum = processNum;
        }

        protected MainThread( Integer processNum) {
            this.processNum = processNum;
        }
        protected static final ExecutorService threadPool = new ThreadPoolExecutor(Runtime.getRuntime().availableProcessors(), Integer.MAX_VALUE, 60L, TimeUnit.SECONDS, new ArrayBlockingQueue<>(1000), new ThreadPoolExecutor.CallerRunsPolicy());

        public void run() {
            KafkaConsumer<String, String> topicConsumer = new KafkaConsumer<String, String>(ConfigUtils.getConsumerProperties(PorpertiesLoggerLevel.INFO));
            //获取分区个数
            int partitionSize = topicConsumer.partitionsFor(ConfigUtils.TOPIC).size();
            consumerNum=consumerNum>partitionSize?partitionSize:consumerNum;
            for (int i = 0; i < consumerNum; i++) {
                threadPool.submit(new ConsumerThread(processNum));
            }
            topicConsumer.close();
        }

    }

    private static class ConsumerThread extends MainThread {
        public ConsumerThread( Integer processNum) {
            super(processNum);
            this.processNum = processNum;
        }

        public void run() {
            KafkaConsumer<String, String> consumer = new KafkaConsumer<>(ConfigUtils.getConsumerProperties());
            consumer.subscribe(Arrays.asList(ConfigUtils.TOPIC));
            try {
                while (true) {
                    ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(3000));
                    if (!consumerRecords.isEmpty()) {
                        threadPool.submit(new ProcessThread(consumerRecords));
                    }
                }
            }catch (Exception e){
                e.printStackTrace();
            }finally {
                consumer.close();
            }
        }

    }

    public static class ProcessThread implements Runnable {
        private  ConsumerRecords<String, String> consumerRecords;

        public ProcessThread(ConsumerRecords consumerRecords) {
            this.consumerRecords = consumerRecords;
        }

        @Override
        public void run() {
            for (ConsumerRecord<String, String> record : consumerRecords) {
                System.out.println(record.value());
            }
        }
    }
}
