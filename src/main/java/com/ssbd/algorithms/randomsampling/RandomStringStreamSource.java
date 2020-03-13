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


import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

/**
 * @ClassName RandomStringStreamSource
 * @Description This class can generate random string with no more than 10 characters constantly.
 * @Author Chaoqi ZHANG
 * @Date 2020/2/26
 */
public class RandomStringStreamSource implements SourceFunction<String> {

    public static final Logger logger = LoggerFactory.getLogger(RandomStringStreamSource.class);
    private long seed;
    private volatile boolean isRunning = true;
    private Random random;

    RandomStringStreamSource() {
        super();
        this.seed = System.currentTimeMillis();
        this.random = new Random(seed);
    }

    RandomStringStreamSource(long seed) {
        super();
        this.seed = seed;
        this.random = new Random(seed);
    }

    @Override
    public void run(SourceContext<String> sourceContext) throws Exception {

        while (isRunning) {
            Thread.sleep(500);

            int n = random.nextInt(10) + 1;
            StringBuffer sb = new StringBuffer();
            for (int i = 0; i < n; i++) {
                int j = random.nextInt(26);
                char t = (char) ('a' + j);
                sb.append(t);
            }

            sourceContext.collect(sb.toString());
        }

    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
