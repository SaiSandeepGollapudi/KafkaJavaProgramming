package demos.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {
    private static final Logger log= LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());

    public static void main(String[] args) {
        log.info("I am a Kafka Producer!");

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
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());//strings will be serialized  using
        // the StringSerializer which is a class provided with the Kafka clients.in Kafka when you use a producer,you pass
        // in some information and at first there will be a string and that will be serialized into bytes by this
        // key.serializer and the value.serializer before being sent to Apache Kafka.


        // create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // create a Producer Record
        ProducerRecord<String, String> producerRecord =
                new ProducerRecord<>("demo_java", "hello world");


        // send data
        producer.send(producerRecord);

        // tell the producer to send all data and block until done -- synchronous
        producer.flush();

        // flush and close the producer
        producer.close();

    }
    }
