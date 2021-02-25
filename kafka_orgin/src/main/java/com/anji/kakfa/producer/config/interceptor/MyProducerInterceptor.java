package com.anji.kakfa.producer.config.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * \* Date: 2021/2/23
 * \* Author: XiaomingZhang
 * \* Desc: 执行过程:拦截器,序列化器,分区器.拦截器主要执行一些消息发送前的过滤工作,连接器可以组成连接器链,在配置时候拦截器使用","分隔开.
 */
public class MyProducerInterceptor implements ProducerInterceptor<String,String>/*k,v类型*/ {

    /**
     * 消息序列化与计算分区之前会调用执行定制化操作,比如在消息上添加一个字符前缀
     * @param producerRecord
     * @return
     */
    @Override
    public ProducerRecord onSend(ProducerRecord producerRecord) {
        return producerRecord;
    }

    /**
     * 消息在被应答之前或者消息发送失败时调用,优先于回调函数CallBack,与CallBack一样两个参数必然页只能有一个为null
     * 在producer线程中执行,不宜逻辑复杂,可以用来统计成功与失败消息个数
     * @param recordMetadata
     * @param e
     */
    @Override
    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {

    }

    /**
     * 资源清理,只有在producer调用close()方法时才会调用
     */
    @Override
    public void close() {
        System.out.println("--------关闭生产者拦截器--------");
    }
    /**
     * 配置信息
     */
    @Override
    public void configure(Map<String, ?> map) {

    }
}
