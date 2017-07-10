package com.yubraj.basic;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * Created by yubrajpokharel on 7/9/17.
 */
public class Producer {

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);

        try{
            for (int i = 300; i < 310; i++) {
                kafkaProducer.send(new ProducerRecord<String, String>("my-first-topic",  //topic
                                                                      //3,                //partition
                                                                      "message-Key-"+Integer.toString(i),          //key
                                                                      "Message Value : "+Integer.toString(i) //value
                                                                     ));
            }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            kafkaProducer.close();
        }

    }

}
