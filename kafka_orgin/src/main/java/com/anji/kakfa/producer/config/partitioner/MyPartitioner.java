package com.anji.kakfa.producer.config.partitioner;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import java.util.Map;

/**
 * \* Date: 2021/2/23
 * \* Author: XiaomingZhang
 * \* Desc: 生产者分区器
 */
public class MyPartitioner implements Partitioner {


    /**
     * 返回分区号
     */
    @Override
    public int partition(String s, Object o, byte[] bytes, Object o1, byte[] bytes1, Cluster cluster/*集群元数据信息*/) {
        return 0;
    }

    /**
     * 关闭分区器时候回收一些资源
     */
    @Override
    public void close() {

    }

    /*
    * 配置信息与初始化数据
    * */
    @Override
    public void configure(Map<String, ?> map) {

    }
}
