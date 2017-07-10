package com.yubraj.basic;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Created by yubrajpokharel on 7/9/17.
 */
public class Consumer {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("group.id", "testGroup");

        KafkaConsumer<String , String > consumer = new KafkaConsumer<String, String>(properties);
        List<String > myTopics = new ArrayList<String>();
        myTopics.add("my-first-topic");

        consumer.subscribe(myTopics);

        try{
            while (true){
                ConsumerRecords<String, String> consumerRecord = consumer.poll(10);
                for(ConsumerRecord<String, String> record: consumerRecord){
                    System.out.println(record.toString());
                }
            }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            consumer.close();
        }
    }
}
