package com.anji.kakfa.consumer.config.interceptor;

import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;

/**
 * \* Date: 2021/2/24
 * \* Author: XiaomingZhang
 * \* Desc: 可以配置过滤器链,参数中以逗号分隔,过滤器链中有异常时,会拉取上一个处理成功的过滤器返回的数据
 */
public class MyConsumerInterceptor implements ConsumerInterceptor<String,String> {

    /**
     * poll方法返回之前,可以对数据进行过滤,丢弃不需要的数据
     * @param records
     * @return
     */
    @Override
    public ConsumerRecords<String, String> onConsume(ConsumerRecords<String, String> records) {
        return records;
    }

    /**
     * 提交偏移量之前
     * @param offsets
     */
    @Override
    public void onCommit(Map<TopicPartition, OffsetAndMetadata> offsets) {

    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
