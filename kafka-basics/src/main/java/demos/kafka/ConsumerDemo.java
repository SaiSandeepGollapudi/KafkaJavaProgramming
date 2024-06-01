package demos.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemo {
   // poll method to get messages from the Kafka broker, and we'll see that the poll method will return data immediately
    // if possible, else will return empty and wait for a timeout until it responds.

   private static final Logger log= LoggerFactory.getLogger(ConsumerDemo.class.getSimpleName());
    //Loggers in Spring Boot offer better control, configurability, performance, and scalability compared to
    // `System.out.println()`. They also facilitate centralized log management for easier monitoring and debugging
    // in production environments

    public static void main(String[] args) {
        log.info("I am a Kafka Consumer!");//Control: Logging frameworks like SLF4J allow you to control the logging
        // levels (INFO, DEBUG, ERROR) and output destinations (console, file, etc.) dynamically without changing the code.
        //Flexibility: You can easily filter and format log messages, and integrate with monitoring tools.

        String groupId="my-java-application";// by externalizing this at the top, we can quickly change it.
        String topic = "demo_java";


        //create Producer properties
        Properties properties= new Properties();
      //  properties.setProperty("key","value");


        //port pairs which are used for establishing an initial connection to the Kafka cluster.
        //connect to local host
        properties.setProperty("bootstrap.servers","127.0.0.1:9092");


        // connect to conductor playground/ remote server
//        properties.setProperty("bootstrap.servers","127.0.0.1:9092");
//        properties.setProperty("bootstrap.servers", "cluster.playground.cdkt.io:9092");
//        properties.setProperty("security.protocol", "SASL_SSL");
//        properties.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"your-username\" password=\"your-password\";");
//        properties.setProperty("sasl.mechanism", "PLAIN");
//

        properties.setProperty("key.deserializer", StringDeserializer.class.getName());// So the producer was serializing,
        // and the consumer deserializes,
        // that means takes bytes and transform them into an actual object. So we need a stringDeserializer.class.getName,
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());

        properties.setProperty("group.id",groupId);

        properties.setProperty("auto.offset.reset", "earliest   ");//Earliest means read from the beginning of my
        // topic.This corresponds to the --from beginning option when we looked at the Kafka CLI, and latest corresponds
        // to, only read the new messages sent from now." So because we wanna read the entire history of our topic,
        // we'll choose earliest.

        // create a consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        //subscribe to a topic
        consumer.subscribe(Arrays.asList(topic));//So we'll pass in a collection of topics.So I have arrays as list and
        // then you can pass in as many topics as you want. So you can say topic one and then topic two and so on.
        // But we only have one topic right now.

while(true){// this is an infinite loop.We'll see how to be a little bit better with infinite stuff in the future,
    // but right now we keep on polling for data infinitely,

    log.info("Polling");

    ConsumerRecords<String, String> records= consumer.poll(Duration.ofMillis(1000));//Duration which is how long we're willing to wait to receive data.
    // duration of milliseconds 1000, means that if there's data.Then we will get it right away,But if Kafka does not
    // have any data for us, we are waiting to wait one second to receive data from Kafka.So this is not, this is in
    // order not to overload Kafka.

    for(ConsumerRecord<String, String> record: records){//for every record in my collection of records.
        log.info("key: " + record.key() + ", Value: " + record.value());
        log.info("Partition: "+ record.partition() + ",Offset: "+ record.offset());//if I actually run my producer demo
        // with keys, for example right now,and start sending some data with my producer demo with keys.As we can see now,
        // the data has been received by the consumer.So we can see the hello worlds, we can see the key,the values if
        // it's not null and so on. So it was reading and being efficient.
    }
}



    }
    }
