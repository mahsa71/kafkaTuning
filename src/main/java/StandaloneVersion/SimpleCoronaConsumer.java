package StandaloneVersion;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;
/*
This class is a single thread consumer to read messeages from kafka and measure:
- TotalRecord
- Commit count number
- TotalTime
- average Time per commit
- average record per second

 */
public class SimpleCoronaConsumer {

    private static Object ArrayList;


    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(SimpleCoronaConsumer.class.getName());
        String topic = "corona";

        int recordCount = 0;
        long startTime= 0;
        long finishedTime =0;
        int commitCount = 0;
        int startTimeCommit= 0;
        int finishedTimeCommit =0;
        boolean runFlag = true;

        ArrayList<Integer> recordsOffsets= new ArrayList<>();
        ArrayList<Integer> timePerCommit= new ArrayList<>();

        // create consumer
        KafkaConsumer<String, String> consumer = createConsumer(topic);

        // subscribe consumer to our topic(s)
        consumer.subscribe(Arrays.asList(topic));


        // poll for new data
        startTime= System.currentTimeMillis();
        while(runFlag){
            startTimeCommit = (int) System.currentTimeMillis();
            ConsumerRecords<String, String> records =
                    consumer.poll(Duration.ofMillis(10000)); // new in Kafka 2.0.0



            if(records.isEmpty()) {
                System.out.println("******** Final INFO ********");
                finishedTime = System.currentTimeMillis();

                float avegRecordPerCommit =get_avg_records(recordsOffsets);
                float avegTimePerCommit = get_avg_list(timePerCommit);
                float avegRecordPerSecond = (avegRecordPerCommit /avegTimePerCommit ) * 1000;

                logger.info("TotalRecord:" + recordCount);
                logger.info("Commit count number:" + commitCount);

                logger.info("TotalTime:" + (finishedTime - startTime) / 1000 + " Seconds");
                logger.info("average Time per commit:" +avegTimePerCommit+ " ms");


                logger.info("average record per commit:" + avegRecordPerCommit );
                logger.info("average record per second:" +avegRecordPerSecond);

                consumer.close();
                runFlag = false;
                System.out.println("****************************");
                break;
            }


            for (ConsumerRecord<String, String> record : records){
                ++recordCount;
                //logger.info("Partition: " + record.partition() + ", Offset:" + record.offset());
                /*
                System.out.println(++recordCount);
                System.out.println("record:"+ record);
                logger.info("Key: " + record.key() + ", Value: " + record.value());
                logger.info("Partition: " + record.partition() + ", Offset:" + record.offset());

                logger.info("Committing offsets...");
                consumer.commitSync();
                logger.info("Offsets have been committed");
                */

            }
            consumer.commitSync();
            commitCount++;
            finishedTimeCommit = (int) System.currentTimeMillis();
            timePerCommit.add(finishedTimeCommit - startTimeCommit);
            recordsOffsets.add(recordCount);
            logger.info("Offsets have been committed "+recordCount);

        }

        }

    //***************************************************************************

    public static KafkaConsumer<String, String> createConsumer(String topic) {

        String bootstrapServers = "node2:9092"; //hostname:port OR IP:port
        String groupId = "c103";

            // create consumer configs
            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");


            properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
            // disable auto commit of offsets

            properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1000000");
            // disable auto commit of offsets fedault=500

            properties.setProperty(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG,Integer.toString(2*1024*1024));//
            //default 1MB

            properties.setProperty(ConsumerConfig.FETCH_MAX_BYTES_CONFIG,Integer.toString(500*1024*1024));
            //default 50MB

            properties.setProperty(ConsumerConfig.FETCH_MIN_BYTES_CONFIG,Integer.toString(1*1024*1024));//*1024*1024
            // default is 1 byte


            //properties.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,"10000");//default =5000



            properties.setProperty(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG,"20000");
            //default=500
            properties.setProperty(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG,"3000");
            //default = 300000

            // create consumer
            KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
            consumer.subscribe(Arrays.asList(topic));

            return consumer;

        }
    //***************************************************************************

    public static float get_avg_records(ArrayList<Integer> offset){
        //returns the average number of records commited in each commit via
        // subtracting the offsets in each commit

        int i = 0;
        int j = 1;
        int tmp = 0;

        ArrayList<Integer> distance = new ArrayList<Integer>();
        while (j <= offset.size() - 1) {
            tmp = offset.get(j) - offset.get(i);
            distance.add(tmp);
            i++;
            j++;
        }

        return get_avg_list(distance);

    }
    //***************************************************************************

    public static float get_avg_list(ArrayList<Integer> aList){
        // returns the average of the Items in the list
        int sum = 0;
        int avg = 0;
        for (int x : aList) {
            sum = x + sum;
        }
        avg = sum / (aList.size()-1);
        return avg;
    }


}

