package com.anji.kakfa.consumer;

import com.anji.kakfa.utils.ConfigUtils;
import com.anji.kakfa.utils.enums.PorpertiesLoggerLevel;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;

/**
 * \* Date: 2021/2/24
 * \* Author: XiaomingZhang
 * \* Desc:多线程消费者
 */
public class MyConsumerForMultiThread {

    public static void main(String[] args){
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(ConfigUtils.getConsumerProperties(PorpertiesLoggerLevel.INFO));
        //获取分区个数
        List<PartitionInfo> partitionInfos = consumer.partitionsFor(ConfigUtils.TOPIC);
        for (int i = 0; i < partitionInfos.size(); i++) {
            new Thread(
                    new Runnable() {
                        @Override
                        public void run() {
                            KafkaConsumer<String, String> threadConsumer = new KafkaConsumer<String, String>(ConfigUtils.getConsumerProperties(PorpertiesLoggerLevel.INFO));
                            threadConsumer.subscribe(Arrays.asList(ConfigUtils.TOPIC));
                            try{
                                while(true){
                                    ConsumerRecords<String, String> records = threadConsumer.poll(Duration.ofMillis(3000));
                                    for (ConsumerRecord<String, String> record : records) {
                                        System.out.println(record.value());
                                    }
                                }
                            }catch (Exception e){
                                e.printStackTrace();
                            }finally{
                                threadConsumer.close();
                            }
                        }
                    }
            ).start();
        }
        consumer.close();
    }
}
