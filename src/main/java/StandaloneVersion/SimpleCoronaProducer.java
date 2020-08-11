package StandaloneVersion;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/*
This class is a Simple Producer to produce our corona tweetsfrom  a directory and measure:
 - Total Time
 - Total number of read messages

 */
public class SimpleCoronaProducer {

    public static void main(String[] args) throws IOException {

        Logger logger = LoggerFactory.getLogger(SimpleCoronaProducer.class.getName());
        String bootstrapServers = "node2:9092"; // Change to IP:9092 OR Host:9092
        String topic = "corona"; // Change Your topic name
        String dirname = "/home/datalab/ghanavati/corona_tweets"; //Change Your Directory of containing files
        // Linux path : /home/datalab/ghanavati/corona_tweets
        //Windows Path : "C:\\Users\\m.ghanavati\\Desktop\\corona"

        KafkaProducer<String, String> producer = createKafkaProducer(bootstrapServers);

        //get the list of file paths
        List<String> files = getAllfilesPath(dirname);

        String line = "";
        long StartTime= 0;
        long nor = 0;
        int counter = 1;
        float totalTime =0;

        //read each file line by line and send it to producer
        for(String filepath: files ){
            System.out.println(String.format("Processing File [ %s / %s ]",counter, files.size()));
            counter++;
            System.out.println(filepath);

            File file = new File(filepath);
            //FastScanner fs = new FastScanner(file);
            BufferedReader br = new BufferedReader(new FileReader(file));
            StartTime = System.currentTimeMillis();

            while ((line = br.readLine()) != null) {
                //nor++;
                //producer send line
                if(!line.isEmpty()) {
                    ProducerRecord<String, String> record =
                            new ProducerRecord<String, String>(topic, line);

                    // send data - asynchronous
                    producer.send(record);
                    nor++;
                }
                /*producer.send(record, new Callback() {
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        // executes every time a record is successfully sent or an exception is thrown
                        if (e == null) {
                            // the record was successfully sent
                            logger.info("Received new metadata. \n" +
                                    "Partition: " + recordMetadata.partition() + "\n" +
                                    "Offset: " + recordMetadata.offset() + "\n" +
                                    "Timestamp: " + recordMetadata.timestamp());
                        } else {
                            logger.error("Error while producing", e);
                        }
                    }
                });*/


            }
            // flush data
            producer.flush();
            long T = System.currentTimeMillis();
            float FileTime = (T-StartTime)/1000;
            totalTime = totalTime + FileTime;
            System.out.println("Time = "+FileTime+" s");
            br.close();

        }
        producer.close();
        //long finishedTime = System.currentTimeMillis();
        System.out.println(String.format("Total Time = %f seconds",totalTime));
        System.out.println("Total record = "+ nor);
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
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "30");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(128*1024)); // 32 KB batch size
        //properties.setProperty(ProducerConfig.MAX_REQUEST_SIZE_CONFIG,"52428");//default 1048576

        // create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        return producer;
    }


    public static List<String>  getAllfilesPath(String directory){
        List<String> filenames;
        List<String> filepaths = new ArrayList<String>();
        // Creates a new File instance by converting the given pathname string
        // into an abstract pathname
        File f = new File(directory);

        // Populates the array with names of files and directories
        filenames = Arrays.asList(f.list());
        String fpath = "" ;
        for(int i = 0; i < filenames.size(); i++){
            fpath = directory + "/" +filenames.get(i); // for windows change the seperator String to "\\"
            filepaths.add(fpath);
        }

        return filepaths;
    }

}
