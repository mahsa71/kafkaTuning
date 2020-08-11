# kafkaTuning
This code contains 4 individual classes for kafka producer and consumer with different properties and little variations for reading/writing messeges to kafka:

* SimpleCoronaConsumer: It is single thread consumer to read masseages from kafka and measure TotalRecord, Commit count number, 
TotalTime, average Time per commit and average record per second.

* SimpleCoronaProducer: It is a Simple Producer to produce our corona tweetsfrom  a directory and measure Total Time, Total number of read messages.

* KafkConsumer: This class is a multi thread consumer to read messages from kafka and measure TotalRecord, Commit count number, Total Time, average Time per commit
and average record per second.

* KafkProducer: This class can be used to read message from other programs via send_mesage method.
