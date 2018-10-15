import jdk.nashorn.internal.parser.JSONParser;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * Created by ashok on 3/11/2018.
 */
public class KafkaProducerDemo {

    public static void main(String[] args){

        // 1. Create properties Object
        Properties properties = new Properties();

        // 2. Set the properties
        properties.setProperty("bootstrap.servers", "192.168.99.100:9092");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());
        properties.setProperty("acks", "1");
        properties.setProperty("retries", "3");
        // linger.ms is used to send the data to kafka for every milliseconds as mentioned.
        properties.setProperty("linger.ms", "1");

        //3. Create Producer Object
        Producer<String, String> producer = new KafkaProducer<String, String>(properties);

        //4. Create Producer Record and send the producer record to Kafka through Producer object.
        for(Integer key =0; key<10; key ++ ){
            ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>("second_topic", Integer.toString(key),"test record " + key);
            producer.send(producerRecord);
        }

        //5. Close the producer object.
        producer.close();
    }
}
