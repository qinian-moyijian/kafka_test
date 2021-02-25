package com.anji.kakfa.producer;

import com.anji.kakfa.utils.ConfigUtils;
import com.anji.kakfa.utils.enums.PorpertiesLoggerLevel;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * \* Date: 2021/2/23
 * \* Author: XiaomingZhang
 * \* Desc: 生产者,线程安全
 */
public class MyProducer {
    public static void main(String[] args) throws InterruptedException {
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(ConfigUtils.getProducerProperties(PorpertiesLoggerLevel.INFO));
        ProducerRecord<String, String> record = new ProducerRecord<String, String>(ConfigUtils.TOPIC, "message");
        try {
            while (true) {
                Thread.sleep(1000);

                ////////////只管发送////////////
                //producer.send(record);

                ////////////异步发送////////////
                // 添加回调函数当kafka有响应时候就会调用,函数回调分区有序,同分区先发送的消息必然先回调
                producer.send(record, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        //recordMetadata与e只能一个为null.
                        if (e != null) {
                            //出现异常保存消息以后再次发送,或者重试
                            e.printStackTrace();
                        } else {
                            System.out.println(recordMetadata.offset());
                        }
                    }
                });

                ////////////同步发送////////////
           /*     Future<RecordMetadata> send = producer.send(record);
                //RecordMetadata中里面包含一些消息的偏移量,分区等信息
                RecordMetadata recordMetadata = send.get();*/
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }
}
