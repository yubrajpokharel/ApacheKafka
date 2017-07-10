package com.yubraj.offset;

import jdk.nashorn.internal.runtime.ECMAException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Created by yubrajpokharel on 7/9/17.
 */
public class ConsumerManualOffset {
    public static void main(String[] args) {
        Properties properties=new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("enable.auto.commit", false);
        properties.put("group.id","test1");

        KafkaConsumer< String, String> consumer=new KafkaConsumer<String, String>(properties);


        ArrayList<String> topics=new ArrayList<String>();
        topics.add("my-first-topic");

        consumer.subscribe(topics);

        try {
            while(true){
                ConsumerRecords<String, String> records = consumer.poll(1000);

                for(ConsumerRecord<String, String> record : records){
                    System.out.println("Record read in KafkaConsumerApp : " +  record.toString());
                    consumer.commitSync();
                }
            }

        } catch (Exception e) {
            System.out.println("Inside exception loop : ");
            e.printStackTrace();
        }finally{
            consumer.close();
        }
    }
}
