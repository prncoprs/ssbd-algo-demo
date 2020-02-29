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

import org.apache.flink.api.java.utils.MultipleParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


/**
 * @ClassName RandomSampling
 * @Description TODO
 * @Author Chaoqi ZHANG
 * @Date 2020/2/26
 */
public class StreamRandomSamplingDemo {
    public static void main(String[] args) throws Exception {

        // Checking input parameters
        final MultipleParameterTool params = MultipleParameterTool.fromArgs(args);

        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        // simulate to generate two random double data sources
        DataStreamSource<Double> set1StreamSource =  env.addSource(new StreamSetSource());
        DataStreamSource<Double> set2StreamSource =  env.addSource(new StreamSetSource());

        set1StreamSource.print("set1 ").setParallelism(1);
        set2StreamSource.print("set2 ").setParallelism(1);



        // execute program
        env.execute("Random Sampling Algorithm Implementation");
    }
}
