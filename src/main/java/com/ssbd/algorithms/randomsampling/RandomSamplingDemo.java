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
 *
 * @Description This class shows a very simple demo procedure of random sampling algorithm. The random sampling
 * algorithm is introduced in section 2.2 and page 36 of <i>Small Summaries for Big Data</i> by
 * <i>Graham Cormode and Ke Yi</i>.
 *
 * This class uses List<Long> to show the main procedure of the algorithm. And in this demo, we use the Long type
 * data to simulate a data set. We generate a sequence from 1 to 10000, and we separate them to odd numbers and even
 * numbers to construct the set1 and set2. Then we do the Update algorithm on these two sets, which leads us to get
 * sample sets of them. Finally we use Merge algorithm to merge the two sample sets and get a final summary of the
 * original set.
 *
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
        List<Long> summaryOfSet2List = ssbdRandomSamplingUpdate(300, set2List);
        DataSet<Long> summaryOfSet2 = env.fromCollection(summaryOfSet2List);

        // Merge summaries of set1 and set2
        List<Long> summaryOfAllSetsList = ssbdRandomSamplingMerge(200,
                summaryOfSet1List, set1List.size(), summaryOfSet2List, set2List.size());
        DataSet<Long> summaryOfAllSets = env.fromCollection(summaryOfAllSetsList);

        // print the result
        summaryOfSet1.print();
        summaryOfSet2.print();
        summaryOfAllSets.print();
        System.out.println("summary number of set1: " + summaryOfSet1.count());
        System.out.println("summary number of set2: " + summaryOfSet2.count());
        System.out.println("summary number of all sets: " + summaryOfAllSets.count());
        System.out.println("summary number from set1: "
                + summaryOfAllSets.filter((FilterFunction<Long>) aLong -> aLong % 2 == 1).count()
                + " summary number from set2: "
                + summaryOfAllSets.filter((FilterFunction<Long>) aLong -> aLong % 2 == 0).count());

        // execute program
//        env.execute("Random Sampling Algorithm Implementation Demo");
    }

    /**
     * This method mainly implement the Update Algorithm 2.4 in <i>Small Summaries for Big Data</i>.
     * @param sampleNum     the cardinality of the output sample set.
     * @param setA          the input set which will be sampled, which is List<Long> type.
     * @return List<Long>   return the sample set of the input set.
     * @throws IllegalArgumentException    if the input parameter is illegal, then throw a exception.
     * @exception IllegalArgumentException if the input parameter is illegal, then throw this exception.
     */
    public static List<Long> ssbdRandomSamplingUpdate(int sampleNum, List<Long> setA) {

        // check input parameters
        if (sampleNum <= 0) {
            throw new IllegalArgumentException("sample number should be positive.");
        }
        if (sampleNum > setA.size()) {
            throw new IllegalArgumentException("sample number should be smaller than the cardinality of setA.");
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

    /**
     * This method mainly implement the Merge Algorithm 2.5 in <i>Small Summaries for Big Data</i>.
     * @param sampleNum     the cardinality of the sample set.
     * @param sampleSet1    the input set 1 which will be merged.
     * @param set1Num       the cardinality of the original set 1.
     * @param sampleSet2    the input set 2 which will be merged.
     * @param set2Num       the cardinality of the original set 2.
     * @return List<Long>   return a sample set which is merged based on two sample sets.
     * @throws IllegalArgumentException     if the input parameter is illegal, then throw a exception.
     * @exception IllegalArgumentException  if the input parameter is illegal, then throw this exception.
     */
    public static List<Long> ssbdRandomSamplingMerge(int sampleNum,
                                                     List<Long> sampleSet1,
                                                     int set1Num,
                                                     List<Long> sampleSet2,
                                                     int set2Num) {

        // check input parameters
        if (sampleNum <= 0) {
            throw new IllegalArgumentException("sample number should be positive.");
        }
        if (sampleNum > sampleSet1.size() + sampleSet2.size()) {
            throw new IllegalArgumentException("sample number should be not greater than the sum of sets cardinality.");
        }
        if (set1Num <= 0 || set2Num <= 0) {
            throw new IllegalArgumentException("the cardinality of original set should be positive.");
        }

        // initiate the parameters
        int n1 = set1Num;
        int n2 = set2Num;
        int k1 = 0;
        int k2 = 0;
        Random random = new Random();
        List<Long> summ = new ArrayList<>();

        // get summary from the two sample sets
        for (int index = 0; index < sampleNum; index++) {
            int j = random.nextInt(n1 + n2);
            if (j < n1) {
                summ.add(sampleSet1.get(k1));
                k1 = k1 + 1;
                n1 = n1 - 1;
            } else {
                summ.add(sampleSet2.get(k2));
                k2 = k1 + 1;
                n2 = n2 - 1;
            }
        }

        return summ;
    }

}
