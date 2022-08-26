package com.flinklearn.wc;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Optional;
import java.util.Properties;
import java.util.Random;

public class URLManager {
    public static void main(String[] args) throws Exception {

        // 1 从Kafka中获取数据
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);

        Properties properties1 = new Properties();
        properties1.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092");
        properties1.put(ConsumerConfig.GROUP_ID_CONFIG, "flink");
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>("step1", new SimpleStringSchema(), properties1);
        DataStreamSource<String> stream = env.addSource(kafkaConsumer);

        // 2 数据转换

        // 2.1 统计数量
        DataStream<Tuple2<String, Integer>> URL_Count_Stream = stream.flatMap(new MyFlatMapper()).keyBy(data -> data.f0).sum(1);
        // 2.2 过滤数量大于1的URL
        DataStream<Tuple2<String, Integer>> URL_Todo = URL_Count_Stream.filter(new URLFilter());
        // 2.3 取出URL
        DataStream<String> target_URL = URL_Todo.map(new URLExtractor());
        target_URL.print();



        // 3 输出数据到Kafka
        Properties properties2 = new Properties();
        properties2.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092");
        Optional<FlinkKafkaPartitioner> ps = Optional.of(new MyPartitioner());
        FlinkKafkaProducer kafkaProducer = new FlinkKafkaProducer("step2", new SimpleStringSchema(), properties2, ps);
        target_URL.addSink(kafkaProducer).setParallelism(3);

        env.execute();
    }

    public static class MyPartitioner extends FlinkKafkaPartitioner {
        /**
         * @param record      正常的记录
         * @param key         KeyedSerializationSchema中配置的key
         * @param value       KeyedSerializationSchema中配置的value
         * @param targetTopic targetTopic
         * @param partitions  partition列表[0, 1, 2, 3, 4]
         * @return partition
         */
        @Override
        public int partition(Object record, byte[] key, byte[] value, String targetTopic, int[] partitions) {
            Random random = new Random();
            return random.nextInt(3);
        }
    }

    // 统计类
    public static class MyFlatMapper implements FlatMapFunction<String, Tuple2<String, Integer>>
    {
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception
        {
            out.collect(new Tuple2<String, Integer>(value, 1));
        }
    }

    // FilterFunction实现类
    public static class URLFilter implements FilterFunction<Tuple2<String, Integer>>
    {
        @Override
        public boolean filter(Tuple2<String, Integer> e) throws Exception {

            return e.f1 == 1;
        }
    }

    // MapFunction实现类
    public static class URLExtractor implements MapFunction<Tuple2<String, Integer>, String>
    {
        @Override
        public String map(Tuple2<String, Integer> e) throws Exception {
            return e.f0;
        }
    }

}