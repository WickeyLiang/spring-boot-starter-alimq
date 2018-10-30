package cn.knowbox.book.alimq.consumer;

import cn.knowbox.book.alimq.annotation.RocketMQBatchMessageListener;
import cn.knowbox.book.alimq.annotation.RocketMQOrderMessageListener;
import com.aliyun.openservices.ons.api.ONSFactory;
import com.aliyun.openservices.ons.api.PropertyKeyConst;
import com.aliyun.openservices.ons.api.batch.BatchConsumer;
import com.aliyun.openservices.ons.api.exception.ONSClientException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.util.StringUtils;

import java.util.Properties;

/**
 * @author jibaole
 * @version 1.0
 * @desc 消费者
 * @date 2018/7/7 下午5:19
 */
@Slf4j
public class MqBatchConsumer implements BeanPostProcessor {

    private Properties properties;
    private BatchConsumer consumer;

    public MqBatchConsumer(Properties properties) {
        if (properties == null || properties.get(PropertyKeyConst.ConsumerId) == null
                || properties.get(PropertyKeyConst.AccessKey) == null
                || properties.get(PropertyKeyConst.SecretKey) == null
                || properties.get(PropertyKeyConst.ONSAddr) == null) {
            throw new ONSClientException("orderConsumer properties not set properly.");
        }
        this.properties = properties;
    }

    public void start() {
        this.consumer = ONSFactory.createBatchConsumer(properties);
        this.consumer.start();
    }

    public void shutdown() {
        if (this.consumer != null) {
            this.consumer.shutdown();
        }
    }

    @Override
    public Object postProcessBeforeInitialization(Object bean, String beanName) throws BeansException {
        return bean;
    }

    /**
     * @Description: 获取所有消费者订阅内容(Topic、Tag)
     * @Param: [bean, beanName]
     * @Author: jibaole
     */
    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
        Class<?> clazz = AopUtils.getTargetClass(bean);
        RocketMQBatchMessageListener annotation = clazz.getAnnotation(RocketMQBatchMessageListener.class);
        if (null != annotation) {
            @SuppressWarnings("rawtypes")
            AbstractBatchMessageListener listener = (AbstractBatchMessageListener) bean;
            String subExpression = StringUtils.arrayToDelimitedString(annotation.tag(), " || ");
            consumer.subscribe(annotation.topic(), subExpression, listener);
        }
        return bean;
    }
}
