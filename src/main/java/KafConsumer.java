import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
/*
This class is a multi thread consumer to read messages from kafka and measure:

- TotalRecord
- Commit count number
- Total Time
- average Time per commit
- average record per second

You change the number of threads with numOfThread
 */

public class KafConsumer {
    private static long T0;
    private static long T1;
    private  String bootstrapServer ="";
    private  String groupId ="";
    private  String topic ="";
    private int durationMs ;
    private int numOfThread ;



    public KafConsumer(){
        this.bootstrapServer = "node2:9092"; // Change to IP:9092 OR Host:9092
        this.groupId ="a28"; // Change your group ID
        this.topic = "corona"; // Change Your topic name
        this.durationMs = 10000;
        this.numOfThread = 1;

    }
    public KafConsumer(String bootstrapServer, String groupId,String topic, int numOfThread ) {
        this.bootstrapServer = bootstrapServer;
        this.groupId =groupId;
        this.topic = topic;
        this.numOfThread = numOfThread;
    }

    //***************************************************************************


    public static void main(String[] args) {
        T0 = System.currentTimeMillis();
        new KafConsumer().run();
    }
    //***************************************************************************
    //***************************************************************************

    private void run() {

         Logger logger = LoggerFactory.getLogger(KafConsumer.class.getName());

         // latch for dealing with multiple threads
        CountDownLatch latch = new CountDownLatch(numOfThread);

        // create the consumer runnable class
        logger.info("Creating the consumer thread");
        /*Runnable myConsumerRunnable = new KafkaConsumerRunnable(
                bootstrapServer,
                groupId,
                topic,
                latch
        );*/

        // create and start the threads

        ArrayList<Thread> threads = new ArrayList<Thread>();
        ArrayList<KafkaConsumerRunnable> myConsumerRunnables = new ArrayList<KafkaConsumerRunnable>();

        for (int threadCounter= 0 ; threadCounter<numOfThread; threadCounter++) {

            myConsumerRunnables.add(new KafkaConsumerRunnable(
                    bootstrapServer,
                    groupId,
                    topic,
                    latch));

            threads.add(new Thread(myConsumerRunnables.get(threadCounter)) );

            threads.get(threadCounter).start();
        }

        // add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {

            logger.info("Caught shutdown hook");

            for(KafkaConsumerRunnable myConsumerRunnable: myConsumerRunnables)
            {
                ((KafkaConsumerRunnable) myConsumerRunnable).shutdown();

                try {
                    latch.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                logger.info("Application has exited");

            }
            T1 = System.currentTimeMillis();
            logger.info("RUN TIME : " + (T1 - T0) / 1000 + " Seconds");
            }));


        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("Application got interrupted", e);
        } finally {
            logger.info("Application is closing");
        }

        //***************************************************************************

    }

    public class KafkaConsumerRunnable implements Runnable {

        private CountDownLatch latch;
        private KafkaConsumer<String, String> consumer;
        private Logger logger = LoggerFactory.getLogger(KafkaConsumerRunnable.class.getName());

        //constructor
        public KafkaConsumerRunnable(String bootstrapServers, String groupId,
                                     String topic, CountDownLatch latch) {
            this.latch = latch;

            // create consumer configs
            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
            // disable auto commit of offsets

            properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1000000");
            // disable auto commit of offsets fedault=500

            properties.setProperty(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG,Integer.toString(2*1024*1024));//
            //default 1MB

            properties.setProperty(ConsumerConfig.FETCH_MAX_BYTES_CONFIG,Integer.toString(500*1024*1024));
            //default 50MB

            properties.setProperty(ConsumerConfig.FETCH_MIN_BYTES_CONFIG,Integer.toString(1*1024*1024));//*1024*1024
            // default is 1 byte

            properties.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,"10000");
            // default =5000

            properties.setProperty(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG,"20000");
            //default=500
            properties.setProperty(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG,"3000");
            //default = 300000

            // create consumer
            this.consumer = new KafkaConsumer<String, String>(properties);
            consumer.subscribe(Arrays.asList(topic));
        }
        //***************************************************************************

        @Override
        public void run() {
            try {
                read_message(consumer);
            } catch (WakeupException e) {
                logger.info("Received shutdown signal!");
            } finally {
                Close();
                // tell our main code we're done with the consumer
                latch.countDown();
            }
        }

        //***************************************************************************

        private void read_message(KafkaConsumer<String, String> consumer) {
            int recordCount =0;
            long startTotalTime = 0 ;
            long finishedTotalTime = 0;
            int commitCount =0 ;
            int startCommitTime = 0 ;
            int finishedCommitTime = 0;
            boolean runFlag = true;

            ArrayList<Integer> recordsOffsets = new ArrayList<>();
            ArrayList<Integer> timePerCommit = new ArrayList<>();

            // poll for new data
            startTotalTime = System.currentTimeMillis();
            while (runFlag) {

                startCommitTime = (int) System.currentTimeMillis();
                ConsumerRecords<String, String> records =
                        consumer.poll(Duration.ofMillis(durationMs)); // new in Kafka 2.0.0



                if (records.isEmpty()) {
                    System.out.println("******** Final INFO ********");
                    finishedTotalTime = System.currentTimeMillis();

                    float avegRecordPerCommit = get_avg_records(recordsOffsets);
                    float avegTimePerCommit = get_avg_list(timePerCommit);
                    float avegRecordPerSecond = (avegRecordPerCommit / avegTimePerCommit) * 1000;

                    logger.info("TotalRecord:" + recordCount);
                    logger.info("Commit count number:" + commitCount);

                    logger.info("TotalTime:" + (finishedTotalTime - startTotalTime) / 1000 + " Seconds");
                    logger.info("average Time per commit:" + avegTimePerCommit + " ms");


                    logger.info("average record per commit:" + avegRecordPerCommit);
                    logger.info("average record per second:" + avegRecordPerSecond);

                    //Close();
                    runFlag = false;
                    System.out.println("****************************");
                    break;
                }


                for (ConsumerRecord<String, String> record : records) {
                    ++recordCount;

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
                finishedCommitTime = (int) System.currentTimeMillis();
                timePerCommit.add(finishedCommitTime - startCommitTime);
                recordsOffsets.add(recordCount);
                logger.info("Offsets have been committed " + recordCount);

            }
        }
        //***************************************************************************

        public void Close() {
            consumer.close();
            //shutdown();
        }
        //***************************************************************************

        public  float get_avg_records(ArrayList<Integer> offset) {
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

        public  float get_avg_list(ArrayList<Integer> aList) {
            // returns the average of the Items in the list
            int sum = 0;
            int avg = 0;
            for (int x : aList) {
                sum = x + sum;
            }
            avg = sum / (aList.size() - 1);
            return avg;
        }


        public void shutdown() {
            // the wakeup() method is a special method to interrupt consumer.poll()
            // it will throw the exception WakeUpException
            consumer.wakeup();
            System.out.println("Shutting Down");
        }
    }
    }
