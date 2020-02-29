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
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.MultipleParameterTool;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

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

        // create a set generator
        DataSet<Long> setGenerator = env.generateSequence(1, 10000).setParallelism(1);

        // create a set1, which only contains odd numbers from 1 to 9999
        DataSet<Long> set1 = setGenerator.filter((FilterFunction<Long>) aLong -> aLong % 2 == 1);

        // create a set2, which only contains even numbers from 2 to 10000
        DataSet<Long> set2 = setGenerator.filter((FilterFunction<Long>) aLong -> aLong % 2 == 0);

        // get summary of set1
        List<Long> set1List = set1.collect();
        List<Long> summaryOfSet1List = ssbdRandomSamplingUpdate(100, set1List);
        DataSet<Long> summaryOfSet1 = env.fromCollection(summaryOfSet1List);

        // get summary of set2
        List<Long> set2List = set2.collect();
        List<Long> summaryOfSet2List = ssbdRandomSamplingUpdate(100, set2List);
        DataSet<Long> summaryOfSet2 = env.fromCollection(summaryOfSet2List);

        // Merge summaries of set1 and set2


        // print the result
        summaryOfSet1.print();
        System.out.println("summary number of set1: " + summaryOfSet1.count());
        System.out.println("summary number of set2: " + summaryOfSet2.count());


        // execute program
//        env.execute("Random Sampling Algorithm Implementation Demo");
    }

    public static List<Long> ssbdRandomSamplingUpdate(int sampleNum, List<Long> setA) {
        // check the input parameters
        if (sampleNum > setA.size()) {
            throw new IllegalArgumentException("sample number should be smaller than the cardinality of setA");
        }

        // initiate the sample set based on the original set
        List<Long> setS = new ArrayList<>();
        // sample set range [0, sampleNum)
        for (int index = 0; index < sampleNum; index++) {
            setS.add(setA.get(index));
        }

        // update the sample set when the original set fulfill the sample set
        Random random = new Random();
        // num range [sampleNum, setA.size)
        for (int num = sampleNum; num < setA.size(); num++) {
            // i range [0, num)
            int i = random.nextInt(num);
            if (i < sampleNum) {
                // i range [0, sampleNum)
                setS.set(i, setA.get(num));
            }
        }

        return setS;
    }

}
