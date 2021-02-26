package com.anji.kakfa.admin;

import org.apache.kafka.common.errors.PolicyViolationException;
import org.apache.kafka.server.policy.CreateTopicPolicy;

import java.util.Map;

/**
 * \* Date: 2021/2/26
 * \* Author: XiaomingZhang
 * \* Desc:创建主题时验证是否合法,
 * \*打包后放到lib目录下,server.properties
 * \*配置create.topic.policy.class.name=com.anji.kakfa.admin.MyCreateTopicPolicy
 */
public class MyCreateTopicPolicy implements CreateTopicPolicy {

    /**
     * 创建主题时broker验证
     * @param requestMetadata
     * @throws PolicyViolationException
     */
    @Override
    public void validate(RequestMetadata requestMetadata) throws PolicyViolationException {

        Integer partitionsNum = requestMetadata.numPartitions();
        if (partitionsNum>1){
            throw new PolicyViolationException("创建主题分区只能为1个");
        }
    }

    /**
     * broker关闭时
     * @throws Exception
     */
    @Override
    public void close() throws Exception {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
