package com.anji.kakfa.consumer;

import com.anji.kakfa.utils.ConfigUtils;
import com.anji.kakfa.utils.enums.PorpertiesLoggerLevel;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Arrays;
import java.util.Date;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * \* Date: 2021/2/23
 * \* Author: XiaomingZhang
 * \* Desc: 消费者,线程不安全,提交偏移量要比当前拉取到的最大偏移量大1
 */
public class MyConsumer {
    //线程安全,并发中只有一个线程可以访问
    private static AtomicBoolean running = new AtomicBoolean(true);

    //批量大小与缓存记录
   /* private static final Integer minBatchNum=100;
    private static ArrayList<ConsumerRecord> recordBuffer = new ArrayList<ConsumerRecord>();*/

   //用来保存需要提交的偏移量,再均衡时候可以使用这个变量来提交
    /*private static Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();*/

    public static void main(String[] args) {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(ConfigUtils.getConsumerProperties(PorpertiesLoggerLevel.INFO));

         //获取指定topic的分区元数据
        /*List<PartitionInfo> partitionInfos = consumer.partitionsFor(ConfigurationUtils.TOPIC);*/

        consumer.subscribe(Arrays.asList(ConfigUtils.TOPIC));

        //再均衡监听器,再均衡发生时会导致消费者不可用,也就无法提交偏移量
        /*consumer.subscribe(Arrays.asList(ConfigurationUtils.TOPIC), new ConsumerRebalanceListener() {
            *//**
             * 再均衡开始之前和消费者停止读取消息之后调用
             * 可以通过这个回调来处理消费者偏移量的提交
             * @param partitions 再均衡前所分配的分区
             *//*
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                consumer.commitSync(offsets);
                offsets.clear();
            }
            *//**
             * 再均衡开始之后和消费者开始读取消息之前调用
             * @param partitions 再均衡前所分配的分区
             *//*
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {

            }
        });*/

        //取消订阅
        /* consumer.unsubscribe();*/

        //消费指定主题分区的数据
        /*consumer.assign(Arrays.asList(new TopicPartition(ConfigurationUtils.TOPIC,0)));*/

        //获取当前已经提交的偏移量与元数据信息
        /*OffsetAndMetadata offsetAndMetadata = consumer.committed(new TopicPartition(ConfigurationUtils.TOPIC, 0));*/

        SimpleDateFormat dateFormat = new SimpleDateFormat("HH:mm:ss.SSSS");
        System.out.println(dateFormat.format(new Date()));
        try {
            while (running.get()) {

                //没有数据时阻塞3秒再返回,有数据时大概就是1秒左右返回
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(3000));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println(record.value());
                }
                System.out.println(dateFormat.format(new Date()));

                //seekxxx(xxx)重置偏移量相关方法,只能重置分配给当前消费者消费分区的偏移量.必须要先poll一次,分区就会分配,再调用seek方法
                /*consumer.seek();*/

                //获取消费者分配到的分区(必须先poll一次之后才能获取到)
                /*Set<TopicPartition> assignment = consumer.assignment();*/

                //从返回集中获取指定分区或者指定topic的数据
                /* records.records()*/

                //暂停某系分区的数据拉取与恢复
                /*consumer.pause(Arrays.asList(new TopicPartition(ConfigurationUtils.TOPIC,0)));
                Set<TopicPartition> paused = consumer.paused();
                consumer.resume(Arrays.asList(new TopicPartition(ConfigurationUtils.TOPIC,0)));*/

                //同步提交与异步提交:同步提交会阻塞拉取请求,异步提交不会阻塞拉取请求
                /*consumer.commitSync();//当前批次调用有参构造可以做到精准一条消息一提交.
                consumer.commitAsync();*/

                //根据时间戳查询大于此时间戳的最近一条message的偏移量
                /*consumer.offsetsForTimes();*/

                //批量处理与批量提交
                /*ConsumerRecords<String, String> recordSet = consumer.poll(Duration.ofMillis(3000));
                for (ConsumerRecord<String, String> record : recordSet) {
                    recordBuffer.add(record);
                }
                if (recordBuffer.size()>=minBatchNum){
                    //处理逻辑.....
                    consumer.commitAsync();
                    recordBuffer.clear();
                }*/
            }
        } catch (Exception e) {
            e.printStackTrace();
        }finally{
            consumer.close();
        }
    }
}
