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
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

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

        // get input data by connecting to the socket
//        DataStream<String> text = env.socketTextStream(hostname, port, "\n");

        // configure the kafka source
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        // only required for Kafka 0.8
        // properties.setProperty("zookeeper.connect", "localhost:2181");
        properties.setProperty("group.id", "ssbd");

        // add the kafka source
        DataStream<String> text = env
                .addSource(new FlinkKafkaConsumer<>("ssbd", new SimpleStringSchema(), properties));


        SingleOutputStreamOperator<Tuple3<Long, Long, String>> sampled = text
                .flatMap(new FlatMapFunction<String, Tuple2<Long, String>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<Long, String>> out) {
                        out.collect(new Tuple2<Long, String>(1L, value));
                    }
                })
                .keyBy(0)
                .flatMap(new SimpleRandomSample());

        // print the results with a single thread, rather than in parallel
        sampled.print().setParallelism(1);

        env.execute("Socket Window WordCount");
    }

    // ------------------------------------------------------------------------

    public static class SimpleRandomSample extends RichFlatMapFunction<Tuple2<Long, String>, Tuple3<Long, Long, String>> {

        private ValueState<Tuple3<Long, Long, String>> state;

        @Override
        public void flatMap(Tuple2<Long, String> input, Collector<Tuple3<Long, Long, String>> out) throws Exception {
            // access the state value
            Tuple3<Long, Long, String> current = state.value();
            System.out.println("--------------Initial---------------");
            System.out.print("current.f0=" + current.f0);
            System.out.print(" current.f1=" + current.f1);
            System.out.println(" current.f2=" + current.f2);

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

            System.out.println("--------------Result---------------");
            System.out.print("current.f0=" + current.f0);
            System.out.print(" current.f1=" + current.f1);
            System.out.println(" current.f2=" + current.f2);
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
        }
    }

    /**
     * Data type for words with count.
     */
    public static class WordWithCount {

        public String word;
        public long count;

        public WordWithCount() {
        }

        public WordWithCount(String word, long count) {
            this.word = word;
            this.count = count;
        }

        @Override
        public String toString() {
            return word + " : " + count;
        }
    }

    public static class SamplePool {

        private long setNum;
        private ArrayList<String> sampleData;

        public SamplePool() {
            this.setNum = 0;
            this.sampleData = new ArrayList<String>();
        }

        public long getSetNum() {
            return this.setNum;
        }

        public long increaseSetNum() {
            this.setNum = this.setNum + 1;
            return this.setNum;
        }

        public int getNumSample() {
            return this.sampleData.size();
        }

        public ArrayList<String> getSampleData() {
            return this.sampleData;
        }

        public boolean addSampleData(String value) {
//            System.out.println("addSampleData: " + value);
            for (String s : this.sampleData) {
                System.out.print(s + " ");
            }
//            System.out.println("sampleData size: " + getNumSample());
            return this.sampleData.add(value);
        }

        public boolean setSampleData(int index, String value) {
//            System.out.println("setSampleData: " + "index=" + index + " value=" + value);
            if (index >= this.getNumSample()) {
                return false;
            }
            this.sampleData.set(index, value);
            return true;
        }
    }

    public static class SampleNode {
        private long index;
        private String value;

        public SampleNode() {
            this.index = -1L;
            this.value = null;
        }

        public SampleNode(long index, String value) {
            this.index = index;
            this.value = value;
        }

        @Override
        public String toString() {
            return index + " : " + value;
        }
    }


    public static class SimpleRandomSampleFunction extends KeyedProcessFunction<Tuple, com.ssbd.algorithms.randomsampling.SocketWindowWordCount.WordWithCount, String> {

        private ValueState<com.ssbd.algorithms.randomsampling.SocketWindowWordCount.SamplePool> state;
        private ListState<com.ssbd.algorithms.randomsampling.SocketWindowWordCount.SampleNode> lstate;

        private ValueState<Tuple2<Long, String>> myState;

        @Override
        public void open(Configuration parameters) throws Exception {
            StateTtlConfig ttlConfig = StateTtlConfig
                    .newBuilder(org.apache.flink.api.common.time.Time.seconds(1))
                    .disableCleanupInBackground()
                    .build();
            ValueStateDescriptor<com.ssbd.algorithms.randomsampling.SocketWindowWordCount.SamplePool> stateDescriptor = new ValueStateDescriptor<>("mySampleState", com.ssbd.algorithms.randomsampling.SocketWindowWordCount.SamplePool.class);
            stateDescriptor.enableTimeToLive(ttlConfig);

            state = getRuntimeContext()
                    .getState(stateDescriptor);
            lstate = getRuntimeContext()
                    .getListState(new ListStateDescriptor<com.ssbd.algorithms.randomsampling.SocketWindowWordCount.SampleNode>("myListState", com.ssbd.algorithms.randomsampling.SocketWindowWordCount.SampleNode.class));

            ValueStateDescriptor<Tuple2<Long, String>> descriptor =
                    new ValueStateDescriptor<>(
                            "myState", // the state name
                            TypeInformation.of(new TypeHint<Tuple2<Long, String>>() {
                            }), // type information
                            Tuple2.of(0L, "")); // default value of the state, if nothing was set
            myState = getRuntimeContext().getState(descriptor);
        }

        @Override
        public void processElement(com.ssbd.algorithms.randomsampling.SocketWindowWordCount.WordWithCount wc, Context ctx, Collector<String> out) throws Exception {

            Tuple2<Long, String> myCurr = myState.value();
//            if(myCurr == null) {
//                myCurr.f0 = 0L;
//                myCurr.f1 = "";
//            }
            myCurr.f0 += 1;
            myCurr.f1 += wc.word;
            myState.update(myCurr);
            System.out.println("myCurr.f0= " + myCurr.f0);
            System.out.println("myCurr.f1= " + myCurr.f1);

//            SampleNode node = new SampleNode(1, wc.word );
//            lstate.add(node);
//            lstate.add(node);
//            lstate.add(node);
//
//            Iterator<SampleNode> itsn = lstate.get().iterator();
//            while(itsn.hasNext()) {
//                System.out.println("itsn: " + itsn.next());
//            }

            com.ssbd.algorithms.randomsampling.SocketWindowWordCount.SamplePool current = state.value();
            if (current == null) {
                current = new com.ssbd.algorithms.randomsampling.SocketWindowWordCount.SamplePool();
            }


            current.increaseSetNum();
//            System.out.println("curr setNum: " + current.getSetNum());

            long sampleSize = 10;
            if (current.getNumSample() < sampleSize) {
                current.addSampleData(wc.word);
            } else {
                long r = (long) (Math.random() * (current.getSetNum()));
                if (r < sampleSize) {
                    current.setSampleData((int) r, wc.word);
                }
            }
            state.update(current);

            StringBuffer sb = new StringBuffer();
            ArrayList<String> data = current.getSampleData();
            for (String s : data) {
                sb.append(" ");
                sb.append(s);
            }
            String res = sb.toString();
            System.out.println("processElement: " + res);

            out.collect(res);
        }
    }
}
