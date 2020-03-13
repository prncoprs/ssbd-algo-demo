/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.ssbd.algorithms.randomsampling;

import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Properties;


/**
 * @ClassName SimpleRandomSampleKafkaStreamDemo
 * @Description This class shows a very simple demo procedure of random sampling algorithm. The random sampling
 * algorithm is introduced in section 2.2 and page 36 of <i>Small Summaries for Big Data</i> by
 * <i>Graham Cormode and Ke Yi</i>.
 * <p>
 * This demo uses DataStream API to implement the procedure of the algorithm.
 * @Author Chaoqi ZHANG
 * @Date 2020/3/11
 */
public class SimpleRandomSampleKafkaStreamDemo {

    public static void main(String[] args) throws Exception {

        // get the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

        // set the topics of the Kafka
        String input1KafkaTopic = "ssbd1";
        String input2KafkaTopic = "ssbd2";
        String sample1KafkaTopic = "ssbd-sample1";
        String sample2KafkaTopic = "ssbd-sample2";
        String mergedKafkaTopic = "ssbd-merged";

        // ATTENTION: if you want to use kafka as source, please uncomment the code below

//        // configure the kafka source1
//        Properties properties = new Properties();
//        properties.setProperty("bootstrap.servers", "localhost:9092");
//
//        // add the kafka source 1
//        DataStream<String> input1 = env
//                .addSource(new FlinkKafkaConsumer<>(input1KafkaTopic, new SimpleStringSchema(), properties));
//
//        // add the kafka source 2
//        DataStream<String> input2 = env
//                .addSource(new FlinkKafkaConsumer<>(input2KafkaTopic, new SimpleStringSchema(), properties));


        // ATTENTION: if you want to use self-constructed random string as source, please do not uncomment the code below

        // simulate generate tow stream string sources
        DataStream<String> input1 = env.addSource(new RandomStringStreamSource(1L));
        DataStream<String> input2 = env.addSource(new RandomStringStreamSource(2L));


        // execute the simple random sample UPDATE algorithm on the stream 1
        DataStream<SamplePool<String>> sampled1 = input1
                .flatMap(new FlatMapFunction<String, Tuple2<Long, String>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<Long, String>> out) throws Exception {
                        out.collect(new Tuple2<Long, String>(1L, value));
                    }
                })
                .keyBy(0)
                .process(new SimpleRandomSampleUpdate(10L));

        // execute the simple random sample UPDATE algorithm on the stream 2
        DataStream<SamplePool<String>> sampled2 = input2
                .flatMap(new FlatMapFunction<String, Tuple2<Long, String>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<Long, String>> out) throws Exception {
                        out.collect(new Tuple2<Long, String>(1L, value));
                    }
                })
                .keyBy(0)
                .process(new SimpleRandomSampleUpdate(15L));

        // execute the simple random sample MERGE algorithm which combine sampled stream 1 with sampled stream 2
        DataStream<SamplePool<String>> merged = sampled1
                .flatMap(new FlatMapFunction<SamplePool<String>, Tuple2<Long, SamplePool<String>>>() {
                    @Override
                    public void flatMap(SamplePool<String> value,
                                        Collector<Tuple2<Long, SamplePool<String>>> out) throws Exception {
                        out.collect(new Tuple2<Long, SamplePool<String>>(1L, value));
                    }
                })
                .keyBy(0)
                .connect(sampled2
                        .flatMap(new FlatMapFunction<SamplePool<String>, Tuple2<Long, SamplePool<String>>>() {
                            @Override
                            public void flatMap(SamplePool<String> value,
                                                Collector<Tuple2<Long, SamplePool<String>>> out) throws Exception {
                                out.collect(new Tuple2<Long, SamplePool<String>>(1L, value));
                            }
                        })
                        .keyBy(0))
                .flatMap(new SimpleRandomSampleMerge(20L));


        // output the streams in the console
        sampled1.print("sampled1: ");
        sampled2.print("sampled2: ");
        merged.print("merged: ");


        // configure the kafka producer properties
        Properties producerConfig = new Properties();
        producerConfig.setProperty("bootstrap.servers", "localhost:9092");

        // add the sampled1, sampled2, merged kafka serialization schemas
        KafkaSerializationSchema<SamplePool<String>> sampled1KafkaSerializationSchema = (KafkaSerializationSchema<SamplePool<String>>)
                (element, timestamp) -> new ProducerRecord<>(sample1KafkaTopic, element.toString().getBytes());
        KafkaSerializationSchema<SamplePool<String>> sampled2KafkaSerializationSchema = (KafkaSerializationSchema<SamplePool<String>>)
                (element, timestamp) -> new ProducerRecord<>(sample2KafkaTopic, element.toString().getBytes());
        KafkaSerializationSchema<SamplePool<String>> mergedKafkaSerializationSchema = (KafkaSerializationSchema<SamplePool<String>>)
                (element, timestamp) -> new ProducerRecord<>(mergedKafkaTopic, element.toString().getBytes());

        // add the sampled 1 kafka producer
        FlinkKafkaProducer<SamplePool<String>> sampled1Producer =
                new FlinkKafkaProducer<>(sample1KafkaTopic,
                        sampled1KafkaSerializationSchema,
                        producerConfig,
                        FlinkKafkaProducer.Semantic.NONE);
        sampled1Producer.setWriteTimestampToKafka(true);
        sampled1.addSink(sampled1Producer);

        // add the sampled 2 kafka producer
        FlinkKafkaProducer<SamplePool<String>> sampled2Producer =
                new FlinkKafkaProducer<>(sample2KafkaTopic,
                        sampled2KafkaSerializationSchema,
                        producerConfig,
                        FlinkKafkaProducer.Semantic.NONE);
        sampled1Producer.setWriteTimestampToKafka(true);
        sampled2.addSink(sampled2Producer);

        // add the merged kafka producer
        FlinkKafkaProducer<SamplePool<String>> mergedProducer =
                new FlinkKafkaProducer<>(mergedKafkaTopic,
                        mergedKafkaSerializationSchema,
                        producerConfig,
                        FlinkKafkaProducer.Semantic.NONE);
        sampled1Producer.setWriteTimestampToKafka(true);
        merged.addSink(mergedProducer);

        // execute
        env.execute("Simple Random Sample Kafka Stream Demo");
    }

    // ----------------------------------------------------------------------

    /**
     * @ClassName SamplePool
     * @Description This class will be used as the sampled pool to store the sampled elements.
     * @Author Chaoqi ZHANG
     * @Date 2020/3/11
     */
    public static class SamplePool<T> implements java.io.Serializable {

        // Stream size recorded
        private long streamSize;

        // Sample pool size
        private long sampleSize;

        // ArrayList to store the sample data
        private ArrayList<T> sampleData;

        // generate random long number
        private RandomDataGenerator random;

        // Use 10 as the sample pool size
        SamplePool() {
            streamSize = 0L;
            sampleSize = 10L;
            sampleData = new ArrayList<T>();
            random = new RandomDataGenerator();
        }

        /**
         * In this constructor we can pass the sample pool size we wanted
         *
         * @param sampleSize The sample pool size.
         */
        SamplePool(long sampleSize) {
            streamSize = 0L;
            this.sampleSize = sampleSize;
            sampleData = new ArrayList<T>();
            random = new RandomDataGenerator();
        }

        // get the stream size
        public long getStreamSize() {
            return streamSize;
        }

        // get the sample pool size
        public long getSampleSize() {
            return sampleSize;
        }

        // get the sample data
        public ArrayList<T> getSampleData() {
            return sampleData;
        }

        // get the size of recorded sampled data
        public int getSampleDataSize() {
            return sampleData.size();
        }

        // set the stream size
        public void setStreamSize(long streamSize) {
            this.streamSize = streamSize;
        }


        /**
         * This method implements the UPDATE Algorithm 2.4 in <i>Small Summaries for Big Data</i>.
         *
         * @param t The data will be updated in the sample pool.
         * @return t The data wanted to update in the sample pool.
         */
        public T update(T t) {

            // increase the stream size per update action
            streamSize += 1L;

            // if the current stored sampled data length is smaller than sample pool size,
            // then we can add the newer sampled data to the data pool (ArrayList<T>) directly.
            if (sampleData.size() < sampleSize) {
                sampleData.add(t);
            } else {
                // Generates a uniformly distributed random long integer between lower and upper (endpoints included).
                long i = random.nextLong(1L, streamSize);

                // if the random number is not greater than the sample pool size, then we replace
                // stored sampled data in data pool with the newer sampled data
                if (i <= sampleSize) {
                    sampleData.set((int) (i - 1), t);
                }
            }

            return t;
        }


        /**
         * This method implements the Merge Algorithm 2.5 in <i>Small Summaries for Big Data</i>.
         *
         * @param other      The other sample pool will be merged.
         * @param mergedSize The size of the new merged sample pool.
         * @return SamplePool<T> The merged SampledPool.
         */
        public SamplePool<T> merge(SamplePool<T> other, long mergedSize) {

            // check the input argument
            if (other == null || other.getSampleData() == null) {
                return this;
            }
            if (mergedSize < 0) {
                throw new IllegalArgumentException("mergedSize should not be smaller than zero.");
            }

            // initiate the variances will be used in the algorithm
            SamplePool<T> sp = new SamplePool<T>(mergedSize);
            ArrayList<T> sp1 = this.getSampleData();
            ArrayList<T> sp2 = other.getSampleData();
            long n1 = this.getStreamSize();
            long n2 = this.getStreamSize();
            int k1 = 0, k2 = 0;

            // here the MERGE algorithm start
            for (int i = 1; i <= mergedSize; i++) {

                // if the source sampled pool run out, then we break the loop
                if (n1 == 0 && n2 == 0) {
                    break;
                }

                // here set the random long number and prevent from the lower bound is bigger than the upper bound
                long j = 1L;
                if (n1 + n2 > 1L) {
                    j = random.nextLong(1, n1 + n2);
                }

                // here update the merged sample pool data
                if (j <= n1) {
                    sp.update(sp1.get(k1));
                    if (k1 < sp1.size() - 1) {
                        k1 += 1;
                    }
                    n1 -= 1;
                } else {
                    sp.update(sp2.get(k2));
                    if (k2 < sp2.size() - 1) {
                        k2 += 1;
                    }
                    n2 -= 1;
                }
            }

            // set the stream size of new merged sample pool
            sp.setStreamSize(this.getStreamSize() + other.getStreamSize());

            return sp;
        }

        @Override
        public String toString() {
            StringBuffer sb = new StringBuffer();
            sb.append(getClass().getSimpleName()).append(" @ ");
            sb.append(" stream size: ").append(streamSize).append(" ");
            sb.append(" sample size: ").append(sampleSize).append(" ");
            sb.append(" sample data size: ").append(getSampleDataSize()).append(" ");
            sb.append(" sample data: [");
            for (T t : sampleData) {
                sb.append(t.toString()).append(", ");
            }
            sb.deleteCharAt(sb.length() - 1);
            sb.deleteCharAt(sb.length() - 1);
            sb.append("]");
            return sb.toString();
        }
    }


    /**
     * @ClassName SimpleRandomSampleUpdate
     * @Description This class extends KeyedProcessFunction to update the random sample in the DataStream.
     * @Author Chaoqi ZHANG
     * @Date 2020/3/13
     */
    public static class SimpleRandomSampleUpdate extends KeyedProcessFunction<
            Tuple,
            Tuple2<Long, String>,
            SamplePool<String>> {

        private ValueState<SamplePool<String>> state;
        private long sampleSize;

        /**
         * The default sample pool size is 10.
         */
        SimpleRandomSampleUpdate() {
            super();
            sampleSize = 10L;
        }

        /**
         * The constructor will receive the parameter of the sample pool size.
         *
         * @param sampleSize The size of sample pool.
         */
        SimpleRandomSampleUpdate(long sampleSize) {
            super();
            this.sampleSize = sampleSize;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            state = getRuntimeContext().getState(new ValueStateDescriptor<>("sample update state",
                    TypeInformation.of(new TypeHint<SamplePool<String>>() {
                    })));
        }

        @Override
        public void processElement(Tuple2<Long, String> value,
                                   Context ctx,
                                   Collector<SamplePool<String>> out) throws Exception {
            SamplePool<String> sampleList = state.value();
            if (sampleList == null) {
                sampleList = new SamplePool<String>(sampleSize);
            }
            sampleList.update(value.f1);
            state.update(sampleList);

            SamplePool<String> sl = state.value();
            out.collect(sl);
        }
    }

    /**
     * @ClassName SimpleRandomSampleMerge
     * @Description This class extends RichCoFlatMapFunction to merge the random sample in the DataStream.
     * @Author Chaoqi ZHANG
     * @Date 2020/3/13
     */
    public static class SimpleRandomSampleMerge extends RichCoFlatMapFunction<
            Tuple2<Long, SamplePool<String>>,
            Tuple2<Long, SamplePool<String>>,
            SamplePool<String>> {

        private ValueState<SamplePool<String>> state1;
        private ValueState<SamplePool<String>> state2;
        private long mergedSize;

        /**
         * The default merged sample pool size is 10.
         */
        SimpleRandomSampleMerge() {
            super();
            mergedSize = 10L;
        }

        /**
         * The constructor will receive the parameter of the merged sample pool size.
         *
         * @param mergedSize The size of merged sample pool.
         */
        SimpleRandomSampleMerge(long mergedSize) {
            super();
            this.mergedSize = mergedSize;
        }


        @Override
        public void open(Configuration config) {
            state1 = getRuntimeContext().getState(new ValueStateDescriptor<>("sample merge state1",
                    TypeInformation.of(new TypeHint<SamplePool<String>>() {
                    })));
            state2 = getRuntimeContext().getState(new ValueStateDescriptor<>("sample merge state2",
                    TypeInformation.of(new TypeHint<SamplePool<String>>() {
                    })));
        }

        @Override
        public void flatMap1(Tuple2<Long, SamplePool<String>> value,
                             Collector<SamplePool<String>> out) throws Exception {
            SamplePool<String> sp = state2.value();
            if (sp != null) {
                SamplePool<String> tmp = value.f1.merge(sp, mergedSize);
                state2.clear();
                out.collect(tmp);
            } else {
                state1.update(value.f1);
            }
        }

        @Override
        public void flatMap2(Tuple2<Long, SamplePool<String>> value,
                             Collector<SamplePool<String>> out) throws Exception {
            SamplePool<String> sp = state1.value();
            if (sp != null) {
                SamplePool<String> tmp = value.f1.merge(sp, mergedSize);
                state1.clear();
                out.collect(tmp);
            } else {
                state2.update(value.f1);
            }
        }
    }

}
