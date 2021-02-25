package com.anji.kakfa.consumer.config.deserializer;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

/**
 * \* Date: 2021/2/23
 * \* Author: XiaomingZhang
 * \* Desc:消费者反序列化
 */
public class MyDeserializer implements Deserializer<Object> {

    /**
     * 配置当前类
     * @param map
     * @param b
     */
    @Override
    public void configure(Map<String, ?> map, boolean b) {

    }

    /**
     * 反序列化
     * @param s
     * @param bytes
     * @return
     */
    @Override
    public Object deserialize(String s, byte[] bytes) {
        return null;
    }

    @Override
    public Object deserialize(String topic, Headers headers, byte[] data) {
        return null;
    }

    /**
     * 关闭当前反序列化器,一般为空
     */
    @Override
    public void close() {

    }
}
