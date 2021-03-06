package cn.knowbox.book.alimq.consumer;

import cn.knowbox.book.alimq.annotation.RocketMQMessageListener;
import cn.knowbox.book.alimq.annotation.RocketMQOrderMessageListener;
import com.aliyun.openservices.ons.api.Consumer;
import com.aliyun.openservices.ons.api.ONSFactory;
import com.aliyun.openservices.ons.api.PropertyKeyConst;
import com.aliyun.openservices.ons.api.exception.ONSClientException;
import com.aliyun.openservices.ons.api.order.OrderConsumer;
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
public class MqOrderConsumer implements BeanPostProcessor {

    private Properties properties;
    private OrderConsumer consumer;

    public MqOrderConsumer(Properties properties) {
        if (properties == null || properties.get(PropertyKeyConst.ConsumerId) == null
                || properties.get(PropertyKeyConst.AccessKey) == null
                || properties.get(PropertyKeyConst.SecretKey) == null
                || properties.get(PropertyKeyConst.ONSAddr) == null) {
            throw new ONSClientException("orderConsumer properties not set properly.");
        }
        this.properties = properties;
    }

    public void start() {
        this.consumer = ONSFactory.createOrderedConsumer(properties);
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
        RocketMQOrderMessageListener annotation = clazz.getAnnotation(RocketMQOrderMessageListener.class);
        if (null != annotation) {
            @SuppressWarnings("rawtypes")
            AbstractOrderMessageListener listener = (AbstractOrderMessageListener) bean;
            String subExpression = StringUtils.arrayToDelimitedString(annotation.tag(), " || ");
            consumer.subscribe(annotation.topic(), subExpression, listener);
        }
        return bean;
    }
}
