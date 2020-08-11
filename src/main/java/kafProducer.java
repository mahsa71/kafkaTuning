import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
/*
This class can be used to read message from other programs via send_mesage method
 */
public class kafProducer {


    String bootStrapServer = "";//node2:9092
    private KafkaProducer<String, String> producer;
    public kafProducer(String bootStrapServer){
        this.bootStrapServer = bootStrapServer;
        initialize(bootStrapServer);
    }

    private void initialize(String booStrapServer){
        this.producer = createKafkaProducer(bootStrapServer);
    }

    public void send_message(String topic, String line){
        ProducerRecord<String, String> record =
                new ProducerRecord<String, String>(topic, line);

        // send data - asynchronous
        producer.send(record);
    }

    public void send_message(String topic, String key, String line){
        ProducerRecord<String, String> record =
                new ProducerRecord<String, String>(topic, key, line);

        // send data - asynchronous
        producer.send(record);
    }
    public static KafkaProducer<String, String> createKafkaProducer(String bootstrapServers){

        // create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create safe Producer
        //properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "1");  //0,all
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");

        /// high throughput producer (at the expense of a bit of latency and CPU usage)
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        //properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "30");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(128*1024)); // 32 KB batch size
        //properties.setProperty(ProducerConfig.MAX_REQUEST_SIZE_CONFIG,"52428");//default 1048576

        // create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        return producer;
    }
    public void flush(){
        producer.flush();
    }
}
