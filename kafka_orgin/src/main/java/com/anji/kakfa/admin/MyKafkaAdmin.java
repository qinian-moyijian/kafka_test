package com.anji.kakfa.admin;

import com.anji.kakfa.utils.ConfigUtils;
import kafka.server.KafkaConfig;
import org.apache.kafka.clients.admin.*;

import java.util.Collections;
import java.util.HashMap;
import java.util.Properties;

/**
 * \* Date: 2021/2/25
 * \* Author: XiaomingZhang
 * \* Desc: kafka主题相关,broker配置相关
 * \*
 */
public class MyKafkaAdmin {
    private static String topicName = "createTopic3";

    public static void main(String[] args) {
        AdminClient admin = getAdmain();
        try {
            //创topic
            //createTopicWithOldApi(topicName);
            createTopicWithNewApi(admin,topicName);

            //删除topic
            /*admin.deleteTopics(Collections.singleton(topicName));*/

            //查看所有topic
           /* ListTopicsResult listTopicsResult = admin.listTopics();
            KafkaFuture<Map<String, TopicListing>> mapKafkaFuture = listTopicsResult.namesToListings();
            Map<String, TopicListing> topicListingMap = mapKafkaFuture.get();
            for (String s : topicListingMap.keySet()) {
                System.out.println(s+"-"+topicListingMap.get(s));
            }*/

            //查看topic附带副本信息
           /* DescribeTopicsResult describeTopicsResult = admin.describeTopics(Collections.singleton(topicName));
            Map<String, TopicDescription> topicDescriptionMap = describeTopicsResult.all().get();
            for (String s : topicDescriptionMap.keySet()) {
                System.out.println(s+"-"+topicDescriptionMap.get(s));
            }*/

           //查看topic参数(会罗列所有参数)
           /* ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, topicName);
            DescribeConfigsResult describeConfigsResult = admin.describeConfigs(Collections.singleton(configResource));
            Map<ConfigResource, Config> configResourceConfigMap = describeConfigsResult.all().get();
            Config config = configResourceConfigMap.get(configResource);
            System.out.println(config);*/

           //更改topic配置
          /*  ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, topicName);
            Config compact = new Config(Collections.singleton(new ConfigEntry("cleanup.policy", "compact")));
            HashMap<ConfigResource, Config> configs = new HashMap<>();
            configs.put(configResource,compact);
             admin.alterConfigs(configs).all().get();*/

            //增加分区
            /*Map<String, NewPartitions> map = new HashMap<String,NewPartitions>();
            NewPartitions newPartitions = NewPartitions.increaseTo(4);
            map.put(topicName,newPartitions);
            admin.createPartitions(map).all().get();*/

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            admin.close();
        }

    }

    /**
     * 获取admin对象
     *
     * @return
     */
    public static AdminClient getAdmain() {
        Properties pop = new Properties();
        pop.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, ConfigUtils.BOOTSTRAP_SERVERS);
        AdminClient admin = KafkaAdminClient.create(pop);
        return admin;
    }

    /**
     * 老版Api创建主题
     *
     * @param topicName
     */
    public static void createTopicWithOldApi(String topicName) {
        String[] conifg = {
                "--zookeeper", "47.116.137.150:2181",
                "--create",
                "--replication-factor", "1",
                "--partitions", "1",
                "--topic", topicName
        };
        kafka.admin.AclCommand.main(conifg);
    }

    /**
     * 新版api创建主题
     *
     * @param topicName
     */
    public static void createTopicWithNewApi(AdminClient admin, String topicName) {
        //创建主题对象
        NewTopic newTopic = new NewTopic(topicName, 3, (short) 1);

        //指定副本分配原则床架主题
        /*Map<Integer, List<Integer>> assign = new HashMap<Integer,List<Integer>>();
        assign.put(0, Arrays.asList(0));
        assign.put(1, Arrays.asList(0));
        assign.put(2, Arrays.asList(0));
        NewTopic newTopic = new NewTopic("createTopic",assign);*/

        //添加主题配置
        HashMap<String, String> configs = new HashMap<>();
        configs.put(KafkaConfig.LogCleanupPolicyProp().replace("log.", ""), ConfigUtils.TOPIC_CLEANUP_POLICY);
        newTopic.configs(configs);

        //创建主题
        CreateTopicsResult result = admin.createTopics(Collections.singleton(newTopic));
        try {
            //可捕获异常
            result.all().get();
        } catch (Exception e) {
            e.printStackTrace();
        }

        admin.close();
    }

    public static void describeTopic() {

    }
}
