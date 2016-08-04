/**
 * 
 */
package com.howbuy.common;

import java.io.IOException;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author qiankun.li
 * 
 */
public class KafkaProducer {

	private Logger logger = LoggerFactory.getLogger(getClass());

	private kafka.javaapi.producer.Producer<String, String> producer;

	private String topic;

	private final Properties props = new Properties();
	
	public KafkaProducer() throws IOException {

		Properties properties = new Properties();
		properties.load(KafkaProducer.class.getClassLoader().getResourceAsStream(
				"sysconf.properties"));
		String mbl = properties.getProperty("kafka.metadata.broker.list");
		String sc = properties.getProperty("kafka.serializer.class");
		String ksc = properties.getProperty("kafka.key.serializer.class");
		String topic = properties.getProperty("kafka.topic");
		
		//此处配置的是kafka的端
		props.put("metadata.broker.list", mbl);
		//配置value的序列化    
		props.put("serializer.class", sc);   
		//配置key的序列化
		props.put("key.serializer.class", ksc);

		producer = new Producer<String, String>(new ProducerConfig(props));
		this.topic = topic;
	
	}


	public void pubMessage(String key, String message) {
		producer.send(new KeyedMessage<String, String>(topic, key, message));
		logger.info("publish mes topic : {}, mes: {}", topic, message);
	}
	
	public static void main(String[] args) {
		try {
			new KafkaProducer().pubMessage("test", "test");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
