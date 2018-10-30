package cn.knowbox.book.alimq;

import cn.knowbox.book.alimq.consumer.MqBatchConsumer;
import cn.knowbox.book.alimq.consumer.MqConsumer;
import cn.knowbox.book.alimq.consumer.MqOrderConsumer;
import cn.knowbox.book.alimq.producer.LocalTransactionCheckerImpl;
import cn.knowbox.book.alimq.producer.OrderMessageTemplate;
import cn.knowbox.book.alimq.producer.RocketMQTemplate;
import cn.knowbox.book.alimq.producer.TransactionMessageTemplate;

import com.aliyun.openservices.ons.api.PropertyKeyConst;
import com.aliyun.openservices.ons.api.bean.OrderProducerBean;
import com.aliyun.openservices.ons.api.bean.ProducerBean;
import com.aliyun.openservices.ons.api.bean.TransactionProducerBean;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.StringUtils;

import java.util.Properties;

/**
 * @author jibaole
 * @version 1.0
 * @desc 初始化(生成|消费)相关配置
 * @date 2018/7/7 下午5:19
 */
@Configuration
@EnableConfigurationProperties(RocketMQProperties.class)
@Slf4j
public class RocketMQAutoConfiguration {
    @Autowired
    private RocketMQProperties propConfig;


    @Bean(name = "producer",initMethod = "start", destroyMethod = "shutdown")
    @ConditionalOnMissingBean
    @ConditionalOnProperty(prefix = "aliyun.mq.producer",value = "enabled",havingValue = "true")
    public ProducerBean producer() {
        ProducerBean producerBean = new ProducerBean();
        Properties properties = new Properties();
        log.info("执行producer初始化……");
        properties.put(PropertyKeyConst.ProducerId, propConfig.getProducer().getProperty("producerId"));
        properties.put(PropertyKeyConst.AccessKey, propConfig.getAccessKey());
        properties.put(PropertyKeyConst.SecretKey, propConfig.getSecretKey());
        setProducerOtherProperties(properties);
        producerBean.setProperties(properties);
        producerBean.start();
        return producerBean;
    }

    private void setProducerOtherProperties(Properties properties) {
        properties.put(PropertyKeyConst.SendMsgTimeoutMillis, propConfig.getProducer().getProperty("sendMsgTimeoutMillis", "3000"));

        if (propConfig.getOnsAddr() != null) {
            properties.put(PropertyKeyConst.ONSAddr, propConfig.getOnsAddr());
        }
        if (propConfig.getNamesrvAddr() != null) {
            properties.put(PropertyKeyConst.NAMESRV_ADDR, propConfig.getNamesrvAddr());
        }


    }

    @Bean(name = "orderProducer",initMethod = "start", destroyMethod = "shutdown")
    @ConditionalOnMissingBean
    @ConditionalOnProperty(prefix = "aliyun.mq.producer",value = "enabled",havingValue = "true")
    public OrderProducerBean orderProducer() {
    	OrderProducerBean producerBean = new OrderProducerBean();
        Properties properties = new Properties();
        log.info("执行producer初始化……");
        properties.put(PropertyKeyConst.ProducerId, propConfig.getProducer().getProperty("producerId"));
        properties.put(PropertyKeyConst.AccessKey, propConfig.getAccessKey());
        properties.put(PropertyKeyConst.SecretKey, propConfig.getSecretKey());
        setProducerOtherProperties(properties);
        properties.put(PropertyKeyConst.SuspendTimeMillis, propConfig.getProducer().getProperty("suspendTimeMillis", "3000"));
        producerBean.setProperties(properties);
        producerBean.start();
        return producerBean;
    }
    
    @Bean(name = "transactionProducer",initMethod = "start", destroyMethod = "shutdown")
    @ConditionalOnMissingBean
    @ConditionalOnProperty(prefix = "aliyun.mq.producer",value = "enabled",havingValue = "true")
    public TransactionProducerBean transactionProducer() {
    	TransactionProducerBean producerBean = new TransactionProducerBean();
        Properties properties = new Properties();
        log.info("执行producer初始化……");
        properties.put(PropertyKeyConst.ProducerId, propConfig.getProducer().getProperty("producerId"));
        properties.put(PropertyKeyConst.AccessKey, propConfig.getAccessKey());
        properties.put(PropertyKeyConst.SecretKey, propConfig.getSecretKey());
        setProducerOtherProperties(properties);
        properties.put(PropertyKeyConst.CheckImmunityTimeInSeconds, propConfig.getProducer().getProperty("checkImmunityTimeInSeconds", "30"));
        producerBean.setProperties(properties);
        //LocalTransactionCheckerImpl必须在start方法调用前设置
        producerBean.setLocalTransactionChecker(new LocalTransactionCheckerImpl(null));
        producerBean.start();
        return producerBean;
    }


    @Bean(initMethod="start", destroyMethod = "shutdown")
    @ConditionalOnMissingBean
    @ConditionalOnProperty(prefix = "aliyun.mq.consumer",value = "enabled",havingValue = "true")
    public MqConsumer mqConsumer(){
        Properties properties = new Properties();
        log.info("执行consumer初始化……");
        properties.setProperty(PropertyKeyConst.ConsumerId, propConfig.getConsumer().getProperty("consumerId"));
        properties.setProperty(PropertyKeyConst.AccessKey, propConfig.getAccessKey());
        properties.setProperty(PropertyKeyConst.SecretKey, propConfig.getSecretKey());
        setConsumerOtherProperties(properties);
        return  new MqConsumer(properties);
    }

    @Bean(initMethod="start", destroyMethod = "shutdown")
    @ConditionalOnMissingBean
    @ConditionalOnProperty(prefix = "aliyun.mq.consumer",value = "enabled",havingValue = "true")
    public MqOrderConsumer mqOrderConsumer(){
        Properties properties = new Properties();
        log.info("执行consumer初始化……");
        properties.setProperty(PropertyKeyConst.ConsumerId, propConfig.getConsumer().getProperty("consumerId"));
        properties.setProperty(PropertyKeyConst.AccessKey, propConfig.getAccessKey());
        properties.setProperty(PropertyKeyConst.SecretKey, propConfig.getSecretKey());
        setConsumerOtherProperties(properties);

        return  new MqOrderConsumer(properties);
    }

    @Bean(initMethod="start", destroyMethod = "shutdown")
    @ConditionalOnMissingBean
    @ConditionalOnProperty(prefix = "aliyun.mq.consumer",value = "enabled",havingValue = "true")
    public MqBatchConsumer mqBatchConsumer(){
        Properties properties = new Properties();
        log.info("执行consumer初始化……");
        properties.setProperty(PropertyKeyConst.ConsumerId, propConfig.getConsumer().getProperty("consumerId"));
        properties.setProperty(PropertyKeyConst.AccessKey, propConfig.getAccessKey());
        properties.setProperty(PropertyKeyConst.SecretKey, propConfig.getSecretKey());
        setConsumerOtherProperties(properties);
        properties.setProperty(PropertyKeyConst.ConsumeMessageBatchMaxSize, propConfig.getConsumer().getProperty("consumeMessageBatchMaxSize", "1"));

        return  new MqBatchConsumer(properties);
    }

    private void setConsumerOtherProperties(Properties properties) {
        if (propConfig.getOnsAddr() != null) {
            properties.put(PropertyKeyConst.ONSAddr, propConfig.getOnsAddr());
        }
        if (propConfig.getNamesrvAddr() != null) {
            properties.put(PropertyKeyConst.NAMESRV_ADDR, propConfig.getNamesrvAddr());
        }
        properties.setProperty(PropertyKeyConst.MessageModel, propConfig.getConsumer().getProperty("messageModel", "CLUSTERING"));
        properties.setProperty(PropertyKeyConst.ConsumeThreadNums, propConfig.getConsumer().getProperty("consumeThreadNums", "4"));
        properties.setProperty(PropertyKeyConst.MaxReconsumeTimes, propConfig.getConsumer().getProperty("maxReconsumeTimes", "16"));
        properties.setProperty(PropertyKeyConst.ConsumeTimeout, propConfig.getConsumer().getProperty("consumeTimeout", "15"));
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(prefix = "aliyun.mq.producer",value = "enabled",havingValue = "true")
    public RocketMQTemplate rocketMQTemplate(){
        RocketMQTemplate rocketMQTemplate = new RocketMQTemplate();
        return rocketMQTemplate;
    }
    
    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(prefix = "aliyun.mq.producer",value = "enabled",havingValue = "true")
    public OrderMessageTemplate orderMessageTemplate(){
    	OrderMessageTemplate orderMessageTemplate = new OrderMessageTemplate();
        return orderMessageTemplate;
    }
    
    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(prefix = "aliyun.mq.producer",value = "enabled",havingValue = "true")
    public TransactionMessageTemplate transactionMessageTemplate(){
    	TransactionMessageTemplate transactionMessageTemplate = new TransactionMessageTemplate();
        return transactionMessageTemplate;
    }
}
