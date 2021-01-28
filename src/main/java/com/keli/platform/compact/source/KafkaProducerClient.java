package com.keli.platform.compact.source;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.kafka.clients.producer.*;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

public class KafkaProducerClient {
    public static void main(String[] args) {
        Config conf = ConfigFactory.load("default.properties");
        String fileInputPath = "data/neima.csv";

        Properties properties = new Properties();

        // kafka集群，broker-list
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, conf.getString("kafka.bootstrap.servers"));

        // acks
        properties.put(ProducerConfig.ACKS_CONFIG, "all");

        /*
         * 重试次数retries
         * 重试间隔retries.backoff.ms
         * 如果一个请求失败了可以重试几次，每次重试的间隔是多少毫秒，根据业务场景需要设置。
         */
        properties.put(ProducerConfig.RETRIES_CONFIG, 1);

        /*
         * batch.size
         * 每个Batch要存放batch.size大小的数据后，才可以发送出去。
         * 比如说batch.size默认值是16KB，那么里面凑够16KB的数据才会发送。
         * 理论上来说，提升batch.size的大小，可以允许更多的数据缓冲在里面，
         * 那么一次Request发送出去的数据量就更多了，这样吞吐量可能会有所提升。
         * 但是batch.size也不能过大，要是数据老是缓冲在Batch里迟迟不发送出去，那么发送消息的延迟就会很高。
         * 一般可以尝试把这个参数调节大些，利用生产环境发消息负载测试一下。
         */
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);

        /*
         * 等待时间linger.ms
         * 一个Batch被创建之后，最多过多久，不管这个Batch有没有写满，都必须发送出去了。
         * 比如说batch.size是16KB，但是现在某个低峰时间段，发送消息量很小。
         * 这会导致可能Batch被创建之后，有消息进来，但是迟迟无法凑够16KB，难道此时就一直等着吗？
         * 当然不是，设置linger.ms是50ms，那么只要这个Batch从创建开始到现在已经过了50ms了，哪怕他还没满16KB，也会被发送出去。
         * linger.ms决定了消息一旦写入一个Batch，最多等待这么多时间，他一定会跟着Batch一起发送出去。
         * linger.ms配合batch.size一起来设置，可避免一个Batch迟迟凑不满，导致消息一直积压在内存里发送不出去的情况。
         */
        properties.put(ProducerConfig.LINGER_MS_CONFIG, 1);

        /*
         * buffer.memory
         * Kafka的客户端发送数据到服务器，不是来一条就发一条，而是经过缓冲的，
         * 也就是说，通过KafkaProducer发送出去的消息都是先进入到客户端本地的内存缓冲里，
         * 然后把很多消息收集成一个一个的Batch，再发送到Broker上去的，这样性能才可能高。
         * buffer.memory的本质就是用来约束KafkaProducer能够使用的内存缓冲的大小的，默认值32MB。
         * 如果buffer.memory设置的太小，可能导致的问题是：
         * 消息快速的写入内存缓冲里，但Sender线程来不及把Request发送到Kafka服务器，会造成内存缓冲很快就被写满。
         * 而一旦被写满，就会阻塞用户线程，不让继续往Kafka写消息了。
         * buffer.memory参数需要结合实际业务情况压测，需要测算在生产环境中用户线程会以每秒多少消息的频率来写入内存缓冲。
         * 经过压测，调试出来一个合理值。
         */
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);

        // key.serializer
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        // value.serializer
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(properties);

        try {
             BufferedReader bf = new BufferedReader(
                     new InputStreamReader(new FileInputStream(fileInputPath), StandardCharsets.UTF_8));
             String line = null;
             while((line = bf.readLine())!=null){
                 // 异步发送不带回调函数
                 //producer.send(new ProducerRecord<>(conf.getString("kafka.topic"), line));

                 /*
                  * 异步发送添加回调函数
                  * 在 producer 收到 ack 时调用，为异步调用
                  * 该方法有两个参数，分别是 RecordMetadata 和 Exception
                  * 如果 Exception 为 null，说明消息发送成功，
                  * 如果 Exception 不为 null，说明消息发送失败。
                  * 注意:消息发送失败会自动重试，不需要我们在回调函数中手动重试。
                  */
                 producer.send(new ProducerRecord<>(conf.getString("kafka.topic"), line),
                         new Callback(){
                             @Override
                             public void onCompletion(RecordMetadata metadata, Exception exception) {
                                 if (exception == null) {
                                     System.out.println("success->" + metadata.partition() + "--" +
                                             metadata.offset());
                                 } else {
                                     exception.printStackTrace();
                                 }
                             }
                         });

                 /*
                  * 同步发送，效率低很少用
                  * send线程发送一条消息，发送过程中会阻塞当前main线程。
                  * 当send线程发送完了会通知main线程，让main线程继续往下运行。
                  * 由于 send 方法返回的是一个 Future 对象，根据 Futrue 对象的特点，我们也可以实现同步发送的效果，
                  * 只需在调用 Future 对象的 get 方发即可。
                  */
                 //producer.send(new ProducerRecord<>(conf.getString("kafka.topic"), line)).get();

             }
             bf.close();
             producer.close();
             System.out.println("已经发送完毕");
        } catch (Exception e) {
             e.printStackTrace();
        }
    }
}
