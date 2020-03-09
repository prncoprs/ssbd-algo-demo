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

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;

/**
 * @ClassName RandomSamplingKafkaStreamDemo
 * @Description TODO
 * @Author Chaoqi ZHANG
 * @Date 2020/3/6
 */

public class RandomSamplingKafkaStreamDemo {

    public static void main(String[] args) throws Exception {

        // get the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);


        // get input data by connecting to the socket
//        DataStream<String> text = env.socketTextStream(hostname, port, "\n");

        // configure the kafka source1
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "ssbd1");

        // add the kafka source 1
        DataStream<String> text1 = env
                .addSource(new FlinkKafkaConsumer<>("ssbd1", new SimpleStringSchema(), properties));

        // configure the kafka source2
//        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "ssbd2");

        // add the kafka source 2
        DataStream<String> text2 = env
                .addSource(new FlinkKafkaConsumer<>("ssbd2", new SimpleStringSchema(), properties));

        // datastream 1 simple random sample update
        SingleOutputStreamOperator<Tuple3<Long, Long, String>> sampled1 = text1
                .flatMap(new FlatMapFunction<String, Tuple2<Long, String>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<Long, String>> out) {
                        out.collect(new Tuple2<Long, String>(1L, value));
                    }
                })
                .keyBy(0)
                .flatMap(new SimpleRandomSampleUpdate());

        // datastream 2 simple random sample update
        SingleOutputStreamOperator<Tuple3<Long, Long, String>> sampled2 = text2
                .flatMap(new FlatMapFunction<String, Tuple2<Long, String>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<Long, String>> out) {
                        out.collect(new Tuple2<Long, String>(1L, value));
                    }
                })
                .keyBy(0)
                .flatMap(new SimpleRandomSampleUpdate());

        // merge sampled datastream 1 and 2
        DataStream<Tuple3<Long, Long, String>> mergedSampled = sampled1.join(sampled2)
                .where(new KeySelector<Tuple3<Long, Long, String>, Long>() {

                    @Override
                    public Long getKey(Tuple3<Long, Long, String> longLongStringTuple3) throws Exception {
                        return 1L;
                    }
                })
                .equalTo(new KeySelector<Tuple3<Long, Long, String>, Long>() {

                    @Override
                    public Long getKey(Tuple3<Long, Long, String> longLongStringTuple3) throws Exception {
                        return 1L;
                    }
                })
                .window(TumblingEventTimeWindows.of(Time.days(1L)))
                .apply(new SimpleRandomSampleMerge(10L));


        // print the results with a single thread, rather than in parallel
        sampled1.print("sample1: ").setParallelism(1);
        sampled2.print("sample2: ").setParallelism(1);
        mergedSampled.print("merged sample: ").setParallelism(1);

        env.execute("Random Sampling Kafka Stream Demo");
    }

    // ------------------------------------------------------------------------

    public static class SimpleRandomSampleMerge implements JoinFunction<Tuple3<Long, Long, String>, Tuple3<Long, Long, String>, Tuple3<Long, Long, String>> {

        public long sampleSize;

        SimpleRandomSampleMerge() {
            sampleSize = 0;
        }

        SimpleRandomSampleMerge(long s) {
            sampleSize = s;
        }

        @Override
        public Tuple3<Long, Long, String> join(Tuple3<Long, Long, String> input1, Tuple3<Long, Long, String> input2) throws Exception {
            long k1 = 1, k2 = 1;
            long n1 = input1.f0;
            long n2 = input2.f0;

            String str1 = input1.f2;
            ArrayList<String> s1 = new ArrayList<String>(Arrays.asList(str1.split(" ")));
            String str2 = input2.f2;
            ArrayList<String> s2 = new ArrayList<String>(Arrays.asList(str2.split(" ")));

            ArrayList<String> strlist = new ArrayList<>();

            for (long i = 0; i < sampleSize; i++) {
                long r = new RandomDataGenerator().nextLong(1L, n1 + n2);
                if ( r <= n1) {
                    strlist.add( s1.get((int) k1 - 1) );
                    k1 += 1;
                    n1 -= 1;
                }
                else {
                    strlist.add( s2.get((int) k2 - 1) );
                    k2 += 1;
                    n2 -= 1;
                }
            }
            String str = StringUtils.join(strlist, " ");
            System.out.println("SimpleRandomSampleMerge: " + str);
            return new Tuple3<Long, Long, String>(input1.f0 + input2.f0, sampleSize, str);
        }
    }


    public static class SimpleRandomSampleUpdate extends RichFlatMapFunction<Tuple2<Long, String>, Tuple3<Long, Long, String>> {

        private ValueState<Tuple3<Long, Long, String>> state;
//        private ValueState<SamplePool> myState;

        @Override
        public void flatMap(Tuple2<Long, String> input, Collector<Tuple3<Long, Long, String>> out) throws Exception {

//            SamplePool curr = myState.value();
//            if (curr == null) {
//                curr = new SamplePool();
//            }
//            curr.increaseSetNum();
//            curr.addSampleData("z");
//            System.out.println("curr setNum: " + curr.getSetNum());
//            System.out.println("curr sampleData: " + curr.getSampleData().toString());

            // access the state value
            Tuple3<Long, Long, String> current = state.value();
//            System.out.println("--------------Initial---------------");
//            System.out.print("current.f0=" + current.f0);
//            System.out.print(" current.f1=" + current.f1);
//            System.out.println(" current.f2=" + current.f2);

            long sampleSize = 10;
            if (current.f0 < sampleSize) {
                current.f1 += 1;
                current.f2 += input.f1;
                current.f2 += " ";
            } else {
                long r = new RandomDataGenerator().nextLong(1L, current.f0);
//                System.out.println(" r= " + r);
                if (r <= sampleSize) {
                    String str = current.f2;
                    ArrayList<String> list = new ArrayList<String>(Arrays.asList(str.split(" ")));
                    list.set((int) r - 1, input.f1);
                    String list_str = StringUtils.join(list, " ");
                    current.f2 = list_str;
                }
            }
            current.f0 += 1;
            // update the state
            state.update(current);

//            System.out.println("--------------Result---------------");
//            System.out.print("current.f0=" + current.f0);
//            System.out.print(" current.f1=" + current.f1);
//            System.out.println(" current.f2=" + current.f2);
            out.collect(current);
        }

        @Override
        public void open(Configuration config) {
            ValueStateDescriptor<Tuple3<Long, Long, String>> descriptor =
                    new ValueStateDescriptor<>(
                            "average", // the state name
                            TypeInformation.of(new TypeHint<Tuple3<Long, Long, String>>() {
                            }), // type information
                            Tuple3.of(0L, 0L, "")); // default value of the state, if nothing was set
            state = getRuntimeContext().getState(descriptor);


//            StateTtlConfig ttlConfig = StateTtlConfig
//                    .newBuilder(org.apache.flink.api.common.time.Time.seconds(1))
//                    .disableCleanupInBackground()
//                    .build();
//            ValueStateDescriptor<SamplePool> stateDescriptor = new ValueStateDescriptor<>("mySampleState", SamplePool.class);
//            stateDescriptor.enableTimeToLive(ttlConfig);
//
//            myState = getRuntimeContext()
//                    .getState(stateDescriptor);
        }
    }
//
//    /**
//     * Data type for words with count.
//     */
//    public static class WordWithCount {
//
//        public String word;
//        public long count;
//
//        public WordWithCount() {
//        }
//
//        public WordWithCount(String word, long count) {
//            this.word = word;
//            this.count = count;
//        }
//
//        @Override
//        public String toString() {
//            return word + " : " + count;
//        }
//    }
//
//    public static class SamplePool {
//
//        private long setNum;
//        private ArrayList<String> sampleData;
//
//        public SamplePool() {
//            this.setNum = 0;
//            this.sampleData = new ArrayList<String>();
//        }
//
//        public long getSetNum() {
//            return this.setNum;
//        }
//
//        public long increaseSetNum() {
//            this.setNum = this.setNum + 1;
//            return this.setNum;
//        }
//
//        public int getNumSample() {
//            return this.sampleData.size();
//        }
//
//        public ArrayList<String> getSampleData() {
//            return this.sampleData;
//        }
//
//        public boolean addSampleData(String value) {
////            System.out.println("addSampleData: " + value);
//            for (String s : this.sampleData) {
//                System.out.print(s + " ");
//            }
////            System.out.println("sampleData size: " + getNumSample());
//            return this.sampleData.add(value);
//        }
//
//        public boolean setSampleData(int index, String value) {
////            System.out.println("setSampleData: " + "index=" + index + " value=" + value);
//            if (index >= this.getNumSample()) {
//                return false;
//            }
//            this.sampleData.set(index, value);
//            return true;
//        }
//    }
//
//    public static class SampleNode {
//        private long index;
//        private String value;
//
//        public SampleNode() {
//            this.index = -1L;
//            this.value = null;
//        }
//
//        public SampleNode(long index, String value) {
//            this.index = index;
//            this.value = value;
//        }
//
//        @Override
//        public String toString() {
//            return index + " : " + value;
//        }
//    }
//
//
//    public static class SimpleRandomSampleFunction extends KeyedProcessFunction<Tuple, WordWithCount, String> {
//
//        private ValueState<SamplePool> state;
//        private ListState<SampleNode> lstate;
//
//        private ValueState<Tuple2<Long, String>> myState;
//
//        @Override
//        public void open(Configuration parameters) throws Exception {
//            StateTtlConfig ttlConfig = StateTtlConfig
//                    .newBuilder(org.apache.flink.api.common.time.Time.seconds(1))
//                    .disableCleanupInBackground()
//                    .build();
//            ValueStateDescriptor<SamplePool> stateDescriptor = new ValueStateDescriptor<>("mySampleState", SamplePool.class);
//            stateDescriptor.enableTimeToLive(ttlConfig);
//
//            state = getRuntimeContext()
//                    .getState(stateDescriptor);
//            lstate = getRuntimeContext()
//                    .getListState(new ListStateDescriptor<SampleNode>("myListState", SampleNode.class));
//
//            ValueStateDescriptor<Tuple2<Long, String>> descriptor =
//                    new ValueStateDescriptor<>(
//                            "myState", // the state name
//                            TypeInformation.of(new TypeHint<Tuple2<Long, String>>() {
//                            }), // type information
//                            Tuple2.of(0L, "")); // default value of the state, if nothing was set
//            myState = getRuntimeContext().getState(descriptor);
//        }
//
//        @Override
//        public void processElement(WordWithCount wc, Context ctx, Collector<String> out) throws Exception {
//
//            Tuple2<Long, String> myCurr = myState.value();
////            if(myCurr == null) {
////                myCurr.f0 = 0L;
////                myCurr.f1 = "";
////            }
//            myCurr.f0 += 1;
//            myCurr.f1 += wc.word;
//            myState.update(myCurr);
//            System.out.println("myCurr.f0= " + myCurr.f0);
//            System.out.println("myCurr.f1= " + myCurr.f1);
//
////            SampleNode node = new SampleNode(1, wc.word );
////            lstate.add(node);
////            lstate.add(node);
////            lstate.add(node);
////
////            Iterator<SampleNode> itsn = lstate.get().iterator();
////            while(itsn.hasNext()) {
////                System.out.println("itsn: " + itsn.next());
////            }
//
//            SamplePool current = state.value();
//            if (current == null) {
//                current = new SamplePool();
//            }
//
//            current.increaseSetNum();
////            System.out.println("curr setNum: " + current.getSetNum());
//
//            long sampleSize = 10;
//            if (current.getNumSample() < sampleSize) {
//                current.addSampleData(wc.word);
//            } else {
//                long r = (long) (Math.random() * (current.getSetNum()));
//                if (r < sampleSize) {
//                    current.setSampleData((int) r, wc.word);
//                }
//            }
//            state.update(current);
//
//            StringBuffer sb = new StringBuffer();
//            ArrayList<String> data = current.getSampleData();
//            for (String s : data) {
//                sb.append(" ");
//                sb.append(s);
//            }
//            String res = sb.toString();
//            System.out.println("processElement: " + res);
//
//            out.collect(res);
//        }
//    }
}
