package com.anji.kakfa.producer.config.serializer;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;
import java.util.Map;

/**
 * \* Date: 2021/2/23
 * \* Author: XiaomingZhang
 * \* Desc: 如果默认序列化器无法满足需求可以,使用自定义序列化或者Avro,Json,Thrift等序列化工具
 */
public class MySerializer implements Serializer<Object> {
    /**
     * 配置当前类
     * @param map
     * @param b
     */
    @Override
    public void configure(Map map, boolean b) {

    }

    /**
     * 执行序列化操作
     * @param s
     * @param o
     * @return
     */
    @Override
    public byte[] serialize(String s, Object o) {
        return new byte[0];
    }

    /**
     *
     * @param topic
     * @param headers 应用相关信息
     * @param data
     * @return
     */
    @Override
    public byte[] serialize(String topic, Headers headers, Object data) {
        return new byte[0];
    }

    /**
     * 关闭序列化器,一般为空
     */
    @Override
    public void close() {

    }
}
