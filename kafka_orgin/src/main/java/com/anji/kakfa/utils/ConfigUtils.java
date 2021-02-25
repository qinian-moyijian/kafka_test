package com.anji.kakfa.utils;

import com.anji.kakfa.utils.enums.PorpertiesLoggerLevel;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * \* Date: 2021/2/23
 * \* Author: XiaomingZhang
 * \* Desc: kafka配置类
 */
public class ConfigUtils {

    private static final Logger logger = LoggerFactory.getLogger(ConfigUtils.class);

    private static final Properties pop;

    private static  Properties producerProperties;

    private static Properties consumerProperties;

    static {
        InputStream resourceAsStream = ConfigUtils.class.getClassLoader().getResourceAsStream("config.properties");
        pop = new Properties();
        try {
            pop.load(resourceAsStream);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * kafka连接地址
     */
    public static final String BOOTSTRAP_SERVERS = getStringConfiguration("bootstrap.servers");
    /**
     * kafka主题
     */
    public static final String TOPIC = getStringConfiguration("topic");
    /**
     * zookeeper地址
     */
    public static final String ZOOKEEPER = getStringConfiguration("zookeeper");

    ////////////////////////////////////////////////  生产者  ///////////////////////////////////////////////////
    /**
     * ack字符串类型:1 leader成功 0不等待返回 -1或者all isr中副本全部写入(默认1)
     */
    public static final String PRODUCER_ACKS = getStringConfiguration("producer.acks");
    /**
     * 生产者重试
     */
    public static final Integer PRODUCER_RETIRES = getIntegerConfiguration("producer.retries");
    /**
     * 生产者重试时间间隔
     */
    public static final Integer PRODUCER_RETIRY_BACKOFF_MS = getIntegerConfiguration("producer.retry.backoff.ms");
    /**
     * 生产者消息缓冲区大小(默认32M)
     */
    public static final Integer PRODUCER_BUFFER_MEMORY = getIntegerConfiguration("producer.buffer.memory");
    /**
     * 发送缓冲区中的每个ProducerBatch的大小(默认16K)
     */
    public static final Integer PRODUCER_BATCH_SIZE = getIntegerConfiguration("producer.batch.size");
    /**
     * ProducerBatch没满之前多少秒发送默认(0)
     */
    public static final Integer PRODUCER_LINGER_MS = getIntegerConfiguration("producer.linger.ms");
    /**
     * 生产者最长阻塞时间
     */
    public static final Integer PRODUCER_MAX_BLOCK_MS = getIntegerConfiguration("producer.max.block.ms");
    /**
     * 生产者分区器
     */
    public static final String PRODUCER_PARTITIONER = getStringConfiguration("producer.partitioner");
    /**
     * 生产者拦截器
     */
    public static final String PRODUCER_INTERCEPTOR = getStringConfiguration("producer.interceptor");
    /**
     * 生产者key序列化
     */
    public static final String PRODUCER_KEY_SERIALIZER = getStringConfiguration("producer.key.serializer");
    /**
     * 生产者value序列化
     */
    public static final String PRODUCER_VALUE_SERIALIZER = getStringConfiguration("producer.value.serializer");
    /**
     * 生产者client id
     */
    public static final String PRODUCER_CLIENT_ID = getStringConfiguration("producer.client.id");
    /**
     * 消息发送成功之前可以发送多少个消息(默认5)
     */
    public static final Integer PRODUCER_MAX_IN_FLIGHT_REQUESTS_PER_CONNECTIONS = getIntegerConfiguration("producer.max.in.flight.requests.per.connection");
    /**
     * 生产者最大一次发送消息总大小(默认1M)
     */
    public static final Integer PRODUCER_MAX_REQUEST_SIZE = getIntegerConfiguration("producer.max.request.size");
    /**
     * 生产者发送消息压缩类型(默认none)
     */
    public static final String PRODUCER_COMPRESSION_TYPE = getStringConfiguration("producer.compression.type");
    /**
     * 生产者连接超时关闭时间(默认9分钟)
     */
    public static final Integer PRODUCER_CONNECTIONS_MAX_IDLE_MS = getIntegerConfiguration("producer.connections.max.idle.ms");
    /**
     * socket接收缓冲区D大小-1为系统默认(默认大小32k)
     */
    public static final Integer PRODUCER_RECEIVE_BUFFER_BYTEST = getIntegerConfiguration("producer.receive.buffer.bytes");
    /**
     * socket发送缓冲区D大小-1为系统默认(默认大小32k)
     */
    public static final Integer PRODUCER_SEND_BUFFER_BYTEST = getIntegerConfiguration("producer.send.buffer.bytes");
    /**
     * producer等待多久没有响应便重新发送(默认30秒)
     */
    public static final Integer PRODUCER_REQUEST_TIMEOUT_MS = getIntegerConfiguration("producer.request.timeout.ms");
    /**
     * 是否开启幂等性(默认false)
     */
    public static final Boolean PRODUCER_ENABLE_IDEMPOTENCE = getBooleanConfiguration("producer.enable.idempotence");
    /**
     * 没有broker变化和分区变化时刷新元数据的间隔时间(默认5分钟)
     */
    public static final Integer PRODUCER_METADATA_MAX_AGE_MS = getIntegerConfiguration("producer.metadata.max.age.ms");

    ////////////////////////////////////////////////  消费者  ///////////////////////////////////////////////////
    /**
     * kafka消费者组
     */
    public static final String CONSUMER_GROUP_ID = getStringConfiguration("consumer.group.id");
    /**
     * kafka消费者组名称
     */
    public static final String CONSUMER_CLIENT_ID = getStringConfiguration("consumer.client.id");
    /**
     * kafka自动提交偏移量
     * 属于同步提交,在poll方法内执行,执行完成之前阻塞拉取请求
     */
    public static final Boolean CONSUMER_ENABLE_AUTO_COMMIT = getBooleanConfiguration("consumer.enable.auto.commit");
    /**
     * kafka自动提交偏移量时间间隔
     */
    public static final Integer CONSUMER_AUTO_COMMIT_INTERVAL_MS = getIntegerConfiguration("consumer.auto.commit.interval.ms");
    /**
     * kafka消费者消费起点策略
     */
    public static final String CONSUMER_AUTO_OFFSET_RESET = getStringConfiguration("consumer.auto.offset.reset");
    /**
     * kafka消费者分区策略
     */
    public static final String CONSUMER_PARTITION_ASSIGNMENT_STRATEGY = getStringConfiguration("consumer.partition.assignment.strategy");
    /**
     * 消费者拦截器
     */
    public static final String CONSUMER_INTERCEPTOR = getStringConfiguration("consumer.interceptor");
    /**
     * 消费者key序列化
     */
    public static final String CONSUMER_KEY_DESERIALIZER = getStringConfiguration("consumer.key.deserializer");
    /**
     * 消费者value序列化
     */
    public static final String CONSUMER_VALUE_DESERIALIZER = getStringConfiguration("consumer.value.deserializer");
    /**
     * 调用poll时,kafka端如果能发送给消费者的数据小于这个值就等待发送(默认1B)
     */
    public static final Integer CONSUMER_FETCH_MIN_BYTES = getIntegerConfiguration("consumer.fetch.min.bytes");
    /**
     * poll时从kafka最大拉取的数据大小(默认50M),如果单一条消息大于这个值,每次只能接受一条消息
     */
    public static final Integer CONSUMER_FETCH_MAX_BYTES =  getIntegerConfiguration("consumer.fetch.max.bytes");
    /**
     * 没有满足最小消息时,等待返回时间(默认500ms)
     */
    public static final Integer CONSUMER_FETCH_MAX_WAIT_MS =  getIntegerConfiguration("consumer.fetch.max.wait.ms");
    /**
     *一次最多从一个分区拉取多少数据(默认1M)
     */
    public static final Integer CONSUMER_MAX_PARTITION_FETCH_BYTES =  getIntegerConfiguration("consumer.max.partition.fetch.bytes");
    /**
     *最大一次poll的数据条数(默认500条)
     */
    public static final Integer CONSUMER_MAX_POLL_RECORDS =  getIntegerConfiguration("consumer.max.poll.records");
    /**
     *多久关闭空置连接(默认9分钟)
     */
    public static final Integer CONSUMER_CONNECTIONS_MAX_IDLE_MS =  getIntegerConfiguration("consumer.connections.max.idle.ms");
    /**
     *通过正则订阅不到内部的topic,只能subscribe(内部主题名)来订阅(默认true)
     */
    public static final Boolean CONSUMER_EXCLUDE_INTERNAL_TOPICS =  getBooleanConfiguration("consumer.exclude.internal.topics");
    /**
     *socket接收消息缓冲区大小(默认64k)
     */
    public static final Integer CONSUMER_RECEIVE_BUFFER_BYTES =  getIntegerConfiguration("consumer.receive.buffer.bytes");
    /**
     *socket发送消息缓冲区大小(默认128k)
     */
    public static final Integer CONSUMER_SEND_BUFFER_BYTES =  getIntegerConfiguration("consumer.send.buffer.bytes");
    /**
     *consumer等待响应的最大等待时间(默认30秒)
     */
    public static final Integer CONSUMER_REQUEST_TIMEOUT_MS =  getIntegerConfiguration("consumer.request.timeout.ms");
    /**
     *没有broker变化和分区变化时刷新元数据的间隔时间
     */
    public static final Integer CONSUMER_METADATA_MAX_AGE_MS =  getIntegerConfiguration("consumer.metadata.max.age.ms");
    /**
     *防止消费者一直向broker发送请求的间隔默认(50毫秒)
     */
    public static final Integer CONSUMER_RECONNECT_BACKOFF_MS =  getIntegerConfiguration("consumer.reconnect.backoff.ms");
    /**
     *配置重新发送失败请求到指定主题之间的间隔(默认100ms)
     */
    public static final Integer CONSUMER_RETRY_BACKOFF_MS =  getIntegerConfiguration("consumer.retry.backoff.ms");
    /**
     *事务隔离级别,read_committed,可以消费LSO的数据(默认read_uncommitted即可以消费HW数据)
     */
    public static final String CONSUMER_ISOLATION_LEVEL =  getStringConfiguration("consumer.isolation.level");
    /**
     *分组管理到消费者协调器的预计时间,必须小于consumer.hearbeat.interval.ms的三分之一
     */
    public static final Integer CONSUMER_HEARTBEAT_INTERVAL_MS =  getIntegerConfiguration("consumer.heartbeat.interval.ms");
    /**
     *组管理协议检测消费者失效时间默认(10秒)
     */
    public static final Integer CONSUMER_SESSION_TIMEOUT_MS =  getIntegerConfiguration("consumer.session.timeout.ms");
    /**
     *如果超过30秒没有poll,则认为consumer离开,触发再均衡(默认30秒)
     */
    public static final Integer CONSUMER_MAX_POLL_INTERVAL_MS =  getIntegerConfiguration("consumer.max.poll.interval.ms");

    /**
     * 获取字符串类型的参数值
     *
     * @param key
     * @return
     */
    public static String getStringConfiguration(String key) {
        String property = pop.getProperty(key);
        return property;

    }

    /**
     * 获取Integer类型的参数值
     *
     * @param key
     * @return
     */
    public static Integer getIntegerConfiguration(String key) {
        Integer i = null;
        try {
            i = Integer.parseInt(pop.getProperty(key));
        } catch (Exception e) {
        }
        return i;
    }

    /**
     * 获取Double类型的参数值
     *
     * @param key
     * @return
     */
    public static Double getDoubleConfiguration(String key) {
        Double i = null;
        try {
            i = Double.parseDouble(pop.getProperty(key));
        } catch (Exception e) {
        }
        return i;
    }

    /**
     * 获取Boolean类型的参数值
     *
     * @param key
     * @return
     */
    public static Boolean getBooleanConfiguration(String key) {
        Boolean i = null;
        try {
            i = Boolean.getBoolean(pop.getProperty(key));
        } catch (Exception e) {
        }
        return i;
    }

    /**
     * 获取全局properties
     *
     * @return
     */
    public static Properties getProperties() {
        return pop;
    }

    /**
     * 获取全局properties并打印log
     *
     * @param loggerLevel 打印级别
     * @return
     */
    public static Properties getProperties(PorpertiesLoggerLevel loggerLevel) {
        loggingConfigs(pop, loggerLevel);
        return pop;
    }

    /**
     * 获取生产者properties
     *
     * @return
     */
    public static Properties getProducerProperties() {
        if (producerProperties == null) {
            producerProperties = new Properties();
            if (BOOTSTRAP_SERVERS!=null) producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
            if (PRODUCER_RETIRES!=null) producerProperties.put(ProducerConfig.RETRIES_CONFIG, PRODUCER_RETIRES);
            if (PRODUCER_PARTITIONER!=null) producerProperties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, PRODUCER_PARTITIONER);
            if (PRODUCER_INTERCEPTOR!=null) producerProperties.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, PRODUCER_INTERCEPTOR);
            if (PRODUCER_KEY_SERIALIZER!=null) producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, PRODUCER_KEY_SERIALIZER);
            if (PRODUCER_VALUE_SERIALIZER!=null) producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, PRODUCER_VALUE_SERIALIZER);
            if (PRODUCER_ACKS!=null) producerProperties.put(ProducerConfig.ACKS_CONFIG, PRODUCER_ACKS);
            if (PRODUCER_CLIENT_ID!=null) producerProperties.put(ProducerConfig.CLIENT_ID_CONFIG, PRODUCER_CLIENT_ID);
            if (PRODUCER_RETIRES!=null) producerProperties.put(ProducerConfig.RETRIES_CONFIG, PRODUCER_RETIRES);
            if (PRODUCER_RETIRY_BACKOFF_MS!=null) producerProperties.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, PRODUCER_RETIRY_BACKOFF_MS);
            if (PRODUCER_MAX_IN_FLIGHT_REQUESTS_PER_CONNECTIONS!=null) producerProperties.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, PRODUCER_MAX_IN_FLIGHT_REQUESTS_PER_CONNECTIONS);
            if (PRODUCER_BUFFER_MEMORY!=null) producerProperties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, PRODUCER_BUFFER_MEMORY);
            if (PRODUCER_BATCH_SIZE!=null) producerProperties.put(ProducerConfig.BATCH_SIZE_CONFIG, PRODUCER_BATCH_SIZE);
            if (PRODUCER_LINGER_MS!=null) producerProperties.put(ProducerConfig.LINGER_MS_CONFIG, PRODUCER_LINGER_MS);
            if (PRODUCER_MAX_BLOCK_MS!=null) producerProperties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, PRODUCER_MAX_BLOCK_MS);
            if (PRODUCER_MAX_REQUEST_SIZE!=null) producerProperties.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, PRODUCER_MAX_REQUEST_SIZE);
            if (PRODUCER_COMPRESSION_TYPE!=null) producerProperties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, PRODUCER_COMPRESSION_TYPE);
            if (PRODUCER_CONNECTIONS_MAX_IDLE_MS!=null) producerProperties.put(ProducerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG, PRODUCER_CONNECTIONS_MAX_IDLE_MS);
            if (PRODUCER_RECEIVE_BUFFER_BYTEST!=null) producerProperties.put(ProducerConfig.RECEIVE_BUFFER_CONFIG, PRODUCER_RECEIVE_BUFFER_BYTEST);
            if (PRODUCER_SEND_BUFFER_BYTEST!=null) producerProperties.put(ProducerConfig.SEND_BUFFER_CONFIG, PRODUCER_SEND_BUFFER_BYTEST);
            if (PRODUCER_REQUEST_TIMEOUT_MS!=null) producerProperties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, PRODUCER_REQUEST_TIMEOUT_MS);
            if (PRODUCER_ENABLE_IDEMPOTENCE!=null) producerProperties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, PRODUCER_ENABLE_IDEMPOTENCE);
            if (PRODUCER_METADATA_MAX_AGE_MS!=null) producerProperties.put(ProducerConfig.METADATA_MAX_AGE_CONFIG, PRODUCER_METADATA_MAX_AGE_MS);
        }
        return producerProperties;
    }

    /**
     * 获取生产者properties并打印log
     *
     * @param loggerLevel 打印级别
     * @return
     */
    public static Properties getProducerProperties(PorpertiesLoggerLevel loggerLevel) {
        Properties producerProperties = getProducerProperties();
        loggingConfigs(ConfigUtils.producerProperties, loggerLevel);
        return producerProperties;
    }

    /**
     * 获取消费者properties
     *
     * @return
     */
    public static Properties getConsumerProperties() {
        if (consumerProperties == null) {
            consumerProperties = new Properties();
            if (BOOTSTRAP_SERVERS!=null) consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
            if (CONSUMER_GROUP_ID!=null) consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP_ID);
            if (CONSUMER_CLIENT_ID!=null) consumerProperties.put(ConsumerConfig.CLIENT_ID_CONFIG, CONSUMER_CLIENT_ID);
            if (CONSUMER_ENABLE_AUTO_COMMIT!=null) consumerProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, CONSUMER_ENABLE_AUTO_COMMIT);
            if (CONSUMER_AUTO_COMMIT_INTERVAL_MS!=null) consumerProperties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, CONSUMER_AUTO_COMMIT_INTERVAL_MS);
            if (CONSUMER_AUTO_OFFSET_RESET!=null) consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, CONSUMER_AUTO_OFFSET_RESET);
            if (CONSUMER_INTERCEPTOR!=null) consumerProperties.put(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, CONSUMER_INTERCEPTOR);
            if (CONSUMER_KEY_DESERIALIZER!=null) consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, CONSUMER_KEY_DESERIALIZER);
            if (CONSUMER_VALUE_DESERIALIZER!=null) consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, CONSUMER_VALUE_DESERIALIZER);
            if (CONSUMER_PARTITION_ASSIGNMENT_STRATEGY!=null) consumerProperties.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CONSUMER_PARTITION_ASSIGNMENT_STRATEGY);
            if (CONSUMER_FETCH_MIN_BYTES!=null) consumerProperties.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, CONSUMER_FETCH_MIN_BYTES);
            if (CONSUMER_FETCH_MAX_BYTES!=null) consumerProperties.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, CONSUMER_FETCH_MAX_BYTES);
            if (CONSUMER_FETCH_MAX_WAIT_MS!=null) consumerProperties.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, CONSUMER_FETCH_MAX_WAIT_MS);
            if (CONSUMER_MAX_PARTITION_FETCH_BYTES!=null) consumerProperties.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, CONSUMER_MAX_PARTITION_FETCH_BYTES);
            if (CONSUMER_MAX_POLL_RECORDS!=null) consumerProperties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, CONSUMER_MAX_POLL_RECORDS);
            if (CONSUMER_CONNECTIONS_MAX_IDLE_MS!=null) consumerProperties.put(ConsumerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG, CONSUMER_CONNECTIONS_MAX_IDLE_MS);
            if (CONSUMER_EXCLUDE_INTERNAL_TOPICS!=null) consumerProperties.put(ConsumerConfig.EXCLUDE_INTERNAL_TOPICS_CONFIG, CONSUMER_EXCLUDE_INTERNAL_TOPICS);
            if (CONSUMER_RECEIVE_BUFFER_BYTES!=null) consumerProperties.put(ConsumerConfig.RECEIVE_BUFFER_CONFIG, CONSUMER_RECEIVE_BUFFER_BYTES);
            if (CONSUMER_SEND_BUFFER_BYTES!=null) consumerProperties.put(ConsumerConfig.SEND_BUFFER_CONFIG, CONSUMER_SEND_BUFFER_BYTES);
            if (CONSUMER_REQUEST_TIMEOUT_MS!=null) consumerProperties.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, CONSUMER_REQUEST_TIMEOUT_MS);
            if (CONSUMER_METADATA_MAX_AGE_MS!=null) consumerProperties.put(ConsumerConfig.METADATA_MAX_AGE_CONFIG, CONSUMER_METADATA_MAX_AGE_MS);
            if (CONSUMER_RECONNECT_BACKOFF_MS!=null) consumerProperties.put(ConsumerConfig.RECONNECT_BACKOFF_MS_CONFIG, CONSUMER_RECONNECT_BACKOFF_MS);
            if (CONSUMER_RETRY_BACKOFF_MS!=null) consumerProperties.put(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG, CONSUMER_RETRY_BACKOFF_MS);
            if (CONSUMER_ISOLATION_LEVEL!=null) consumerProperties.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, CONSUMER_ISOLATION_LEVEL);
            if (CONSUMER_HEARTBEAT_INTERVAL_MS!=null) consumerProperties.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, CONSUMER_HEARTBEAT_INTERVAL_MS);
            if (CONSUMER_SESSION_TIMEOUT_MS!=null) consumerProperties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, CONSUMER_SESSION_TIMEOUT_MS);
            if (CONSUMER_MAX_POLL_INTERVAL_MS!=null) consumerProperties.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, CONSUMER_MAX_POLL_INTERVAL_MS);
        }
        return consumerProperties;
    }

    /**
     * 获取消费者properties并打印log
     * @param loggerLevel 日志级别
     * @return
     */
    public static Properties getConsumerProperties(PorpertiesLoggerLevel loggerLevel) {
        Properties consumerProperties = getConsumerProperties();
        loggingConfigs(consumerProperties,loggerLevel);
        return consumerProperties;
    }

    private static void loggingConfigs(Properties pop, PorpertiesLoggerLevel loggerLevel) {
        StringBuffer buffer = new StringBuffer();
        buffer.append("\n配置信息:\n");
        buffer.append("----------------------------------------------------------------\n");
        for (Object key : pop.keySet()) {
            buffer.append(key);
            buffer.append(" = ");
            buffer.append(pop.get(key));
            buffer.append("\n");
        }
        buffer.append("----------------------------------------------------------------\n");
        String configs = buffer.toString();
        switch (loggerLevel) {
            case DEBUG:
                logger.debug(configs);
                break;
            case INFO:
                logger.info(configs);
                break;
        }
    }

}
