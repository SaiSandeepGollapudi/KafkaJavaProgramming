package demos.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {
    //how to write first Kafka producer using Java API, code to send data to Apache Kafka.We'll view some basic
    // configuration parameters, and we'll confirm that we received the data in a Kafka Console Consumer.
    private static final Logger log= LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());
    //Loggers in Spring Boot offer better control, configurability, performance, and scalability compared to
    // `System.out.println()`. They also facilitate centralized log management for easier monitoring and debugging
    // in production environments

    public static void main(String[] args) {
        log.info("I am a Kafka Producer!");//Control: Logging frameworks like SLF4J allow you to control the logging
        // levels (INFO, DEBUG, ERROR) and output destinations (console, file, etc.) dynamically without changing the code.
        //Flexibility: You can easily filter and format log messages, and integrate with monitoring tools.


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

        //set producer properties
properties.setProperty("key.serializer", StringSerializer.class.getName());//That means that our producer is expecting strings
//        and these strings will be serialized  using the StringSerializer which is a class provided with the Kafka clients.
        properties.setProperty("value.serializer", StringSerializer.class.getName());//strings will be serialized  using
        // the StringSerializer which is a class provided with the Kafka clients.in Kafka when you use a producer,you pass
        // in some information and at first there will be a string and that will be serialized into bytes by this
        // key.serializer and the value.serializer before being sent to Apache Kafka.


        // create the Producer
KafkaProducer<String, String> producer = new KafkaProducer<>(properties);//we've created a producer object from a Kafka
// producer class by passing in the properties and the properties tell how to connect to Kafka and how to serialize values
// and keys.


        // create a Producer Record
        ProducerRecord<String, String> producerRecord =
                new ProducerRecord<>("demo_java", "hello world");


        // send data. using the Java API, we're going to look at callbacks. For us to understand from the producer itself
        //to which partition and offset the message was sent to, we'll use the callback interface.
        producer.send(producerRecord);

        // tell the producer to send all data and block until done -- synchronous
        producer.flush();//when you do producer.close(),it will also call producer.flush() before doing it.But I wanted to
        // highlight the fact that producer.flush() exists as an API if you need to.So when you send data as a producer,
        // it sends it asynchronously.So if I'd never entered these two lines, my program would've just completed
        // without having let the producer the chance to send data to Kafka. But by telling it to flush it, it was
        // sending the data out to Kafka and then by closing it, we got able to make sure
       // that everything was flushed before we finished our program.


                // flush and close the producer
        producer.close();

    }
    }
