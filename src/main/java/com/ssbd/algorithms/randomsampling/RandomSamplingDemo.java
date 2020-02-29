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

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.MultipleParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


import java.util.ArrayList;
import java.util.List;

/**
 * @ClassName RandomSamplingDemo
 * @Description TODO
 * @Author Chaoqi ZHANG
 * @Date 2020/2/29
 */
public class RandomSamplingDemo {
    public static void main(String[] args) throws Exception {

        // Checking input parameters
        final MultipleParameterTool params = MultipleParameterTool.fromArgs(args);

        // set up the streaming execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        DataSet<Long> setGenerator = env.generateSequence(1, 10000).setParallelism(1);

        DataSet<Long> set1 = setGenerator.filter(new FilterFunction<Long>() {
            @Override
            public boolean filter(Long aLong) throws Exception {
                return aLong % 2 == 1;
            }
        });

        DataSet<Long> set2 = setGenerator.filter(new FilterFunction<Long>() {
            @Override
            public boolean filter(Long aLong) throws Exception {
                return aLong % 2 == 0;
            }
        });

//        List<Long> set1List = set1.collect();
//        List<Long> set2List = set2.collect();
//
//        List<Long> samplesOfSet1 = new ArrayList<>();
//
//        samplesOfSet1.add(1L);
//        samplesOfSet1.add(2L);
//        for( Long a = 3L; a < 10000L; a++) {
//            samplesOfSet1.add(a);
//        }
//
//        DataSet<Long> summaryOfSet1 = env.fromCollection(samplesOfSet1);

        DataSet<Long> summaryOfSet1 = set1.filter(new FilterFunction<Long>() {
            @Override
            public boolean filter(Long aLong) throws Exception {

                return false;
            }
        })

        summaryOfSet1.print();

        System.out.println("summary of set1: " + summaryOfSet1.count());

        Thread.sleep(30000);
//        set1List.forEach(item -> {
//            if((item.intValue() + 1) % 10 == 0) {
//                System.out.println("");
//            }
//            System.out.print(item + " ");
//        });

//        System.out.println(set1list);

//        set1.print();
//        set2.print();




        // execute program
//        env.execute("Random Sampling Algorithm Implementation Demo");
    }
}
