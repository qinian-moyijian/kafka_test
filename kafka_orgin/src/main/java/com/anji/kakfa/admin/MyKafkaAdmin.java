package com.anji.kakfa.admin;

/**
 * \* Date: 2021/2/25
 * \* Author: XiaomingZhang
 * \* Desc: kafka创建主题,相当于调用kafka-topic.sh
 */
public class MyKafkaAdmin {
    public static void main(String[] args) {
        String[] conifg={
                "--zookeeper","47.116.137.150:2181",
                "--create",
                "--replication-factor","1",
                "--partitions","1",
                "--topic","topicAPI"
        };
        kafka.admin.AclCommand.main(conifg);
    }
}
