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

package org.apache.flink.test.streaming.api.datastream;

import org.apache.flink.api.common.watermark.Watermark;
import org.apache.flink.api.common.watermark.WatermarkHandlingResult;
import org.apache.flink.api.connector.dsv2.DataStreamV2SourceUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.datastream.api.ExecutionEnvironment;
import org.apache.flink.datastream.api.common.Collector;
import org.apache.flink.datastream.api.context.NonPartitionedContext;
import org.apache.flink.datastream.api.context.PartitionedContext;
import org.apache.flink.datastream.api.function.OneInputStreamProcessFunction;
import org.apache.flink.datastream.api.stream.NonKeyedPartitionStream.ProcessConfigurableAndNonKeyedPartitionStream;
import org.apache.flink.datastream.impl.ExecutionEnvironmentImpl;

import org.junit.jupiter.api.Test;

import java.io.Serializable;
import java.util.List;

public class WatermarkEventTimeITCase implements Serializable {

    /** Parallelism of all operators. */
    private static final int DEFAULT_PARALLELISM = 2;

    @Test
    public void testEventTime() throws Exception {
        ExecutionEnvironmentImpl env =
                (ExecutionEnvironmentImpl) ExecutionEnvironment.getInstance();
        //        env.getConfiguration().set(PipelineOptions.OPERATOR_CHAINING, false);
        ProcessConfigurableAndNonKeyedPartitionStream<Tuple2<String, Long>> source =
                env.fromSource(
                                DataStreamV2SourceUtils.fromData(
                                        List.of(
                                                Tuple2.of("hello", 1L),
                                                Tuple2.of("world", 2L),
                                                Tuple2.of("and", 3L),
                                                Tuple2.of("you", 4L),
                                                Tuple2.of("known", 5L))),
                                "Operator1")
                        .withParallelism(DEFAULT_PARALLELISM);

        source.extractEventTime(element -> element.f1)
                .process(
                        new OneInputStreamProcessFunction<Tuple2<String, Long>, String>() {
                            @Override
                            public void processRecord(
                                    Tuple2<String, Long> record,
                                    Collector<String> output,
                                    PartitionedContext ctx)
                                    throws Exception {}

                            @Override
                            public WatermarkHandlingResult onWatermark(
                                    Watermark watermark,
                                    Collector<String> output,
                                    NonPartitionedContext<String> ctx) {
                                System.out.println(
                                        ctx.getTaskInfo().getTaskName()
                                                + "  "
                                                + ctx.getTaskInfo().getIndexOfThisSubtask()
                                                + " receive Watermark: "
                                                + watermark);
                                return WatermarkHandlingResult.PEEK;
                            }
                        });
        env.execute("test");
    }

    @Test
    public void testEventTimer() throws Exception {
        ExecutionEnvironmentImpl env =
                (ExecutionEnvironmentImpl) ExecutionEnvironment.getInstance();
        //        env.getConfiguration().set(PipelineOptions.OPERATOR_CHAINING, false);
        ProcessConfigurableAndNonKeyedPartitionStream<Tuple2<String, Long>> source =
                env.fromSource(
                                DataStreamV2SourceUtils.fromData(
                                        List.of(
//                                                Tuple2.of("hello", 1L),
//                                                Tuple2.of("world", 2L),
//                                                Tuple2.of("and", 3L),
//                                                Tuple2.of("you", 4L),
                                                Tuple2.of("known", 5L))),
                                "Operator1")
                        .withParallelism(DEFAULT_PARALLELISM);

        source
                .keyBy(element -> element.f0)
                .process(
                        new OneInputStreamProcessFunction<Tuple2<String, Long>, String>() {
                            @Override
                            public void processRecord(
                                    Tuple2<String, Long> record,
                                    Collector<String> output,
                                    PartitionedContext ctx)
                                    throws Exception {
                                long currentEventTime = ctx.getEventTimeManager().currentTime();
                                System.out.println(ctx.getTaskInfo().getTaskName() + ctx.getTaskInfo().getIndexOfThisSubtask()
                                        + ", event time:" + currentEventTime + ", receive record " + record);
                                ctx.getProcessingTimeManager().registerTimer(0);
                                ctx.getEventTimeManager().registerTimer(currentEventTime + 1);
                            }

                            @Override
                            public WatermarkHandlingResult onWatermark(
                                    Watermark watermark,
                                    Collector<String> output,
                                    NonPartitionedContext<String> ctx) {
                                System.out.println(
                                        ctx.getTaskInfo().getTaskName() + ctx.getTaskInfo().getIndexOfThisSubtask()
                                                + ",  "
                                                + " receive Watermark: "
                                                + watermark);
                                return WatermarkHandlingResult.PEEK;
                            }

                            @Override
                            public void onEventTimer(
                                    long timestamp,
                                    Collector<String> output,
                                    PartitionedContext ctx) {
                                System.out.println(ctx.getTaskInfo().getTaskName() + ctx.getTaskInfo().getIndexOfThisSubtask()
                                        + "  "
                                + " onEventTimer timestamp: " + timestamp + ", EventTimeManager time: " + ctx.getEventTimeManager().currentTime()
                                        );
                            }

                            @Override
                            public void onProcessingTimer(
                                    long timestamp,
                                    Collector<String> output,
                                    PartitionedContext ctx) {
                                System.out.println("aaaaa");
                                ctx.getProcessingTimeManager().registerTimer(timestamp + 1);
                            }
                        });
        env.execute("test");
    }
}
