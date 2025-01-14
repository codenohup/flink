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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.dsv2.WrappedSink;
import org.apache.flink.api.connector.dsv2.WrappedSource;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.datastream.api.ExecutionEnvironment;
import org.apache.flink.datastream.api.builtin.BuiltinFuncs;
import org.apache.flink.datastream.api.common.Collector;
import org.apache.flink.datastream.api.context.PartitionedContext;
import org.apache.flink.datastream.api.context.TwoOutputPartitionedContext;
import org.apache.flink.datastream.api.extension.eventtime.EventTimeExtension;
import org.apache.flink.datastream.api.extension.window.context.OneInputWindowContext;
import org.apache.flink.datastream.api.extension.window.context.TwoInputWindowContext;
import org.apache.flink.datastream.api.extension.window.function.OneInputWindowStreamProcessFunction;
import org.apache.flink.datastream.api.extension.window.function.TwoInputNonNroadcastWindowStreamProcessFunction;
import org.apache.flink.datastream.api.extension.window.function.TwoOutputWindowStreamProcessFunction;
import org.apache.flink.datastream.api.extension.window.strategy.WindowStrategy;
import org.apache.flink.datastream.api.function.OneInputStreamProcessFunction;
import org.apache.flink.datastream.api.function.TwoInputNonBroadcastStreamProcessFunction;
import org.apache.flink.datastream.api.function.TwoOutputStreamProcessFunction;
import org.apache.flink.datastream.api.stream.GlobalStream;
import org.apache.flink.datastream.api.stream.NonKeyedPartitionStream;
import org.apache.flink.streaming.api.functions.sink.PrintSink;

import org.junit.jupiter.api.Test;

import java.io.Serializable;
import java.time.Duration;

public class WindowOperatorTest implements Serializable {
    // ===================================================
    //                  |  global stream  | keyed stream  | non keyed stream
    //  global window   |     ok.          | ok.          | ok
    //  time window   |     ?          | ok.          | ok
    //  session window |     ?         |.       ok    |.    ok
    // ===================================================
    @Test
    void testProcess() throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getInstance();

        NonKeyedPartitionStream.ProcessConfigurableAndNonKeyedPartitionStream<ValueWithTimestamp>
                source =
                        env.fromSource(
                                new WrappedSource<ValueWithTimestamp>(
                                        new DataGeneratorSource<ValueWithTimestamp>(
                                                new TestGeneratorFunction(),
                                                100_000,
                                                TypeInformation.of(ValueWithTimestamp.class))),
                                "source");

        NonKeyedPartitionStream.ProcessConfigurableAndNonKeyedPartitionStream<ValueWithTimestamp>
                stream1 =
                        source.process(
                                EventTimeExtension.<ValueWithTimestamp>newWatermarkGeneratorBuilder(
                                                element -> element.getTimestamp())
                                        .perEventWatermark()
                                        .buildAsProcessFunction());

        OneInputStreamProcessFunction<ValueWithTimestamp, String> windowProcessFunction =
                BuiltinFuncs.window(
                        WindowStrategy.tumbling(Duration.ofSeconds(5)),
                        new OneInputWindowStreamProcessFunction<ValueWithTimestamp, String>() {

                            @Override
                            public void onTrigger(
                                    Collector<String> output,
                                    PartitionedContext<String> ctx,
                                    OneInputWindowContext<ValueWithTimestamp> windowContext)
                                    throws Exception {
                                Iterable<ValueWithTimestamp> allRecords =
                                        windowContext.getAllRecords();
                                String result = "";
                                for (ValueWithTimestamp valueWithTimestamp : allRecords) {
                                    result += valueWithTimestamp.getValue() + ",";
                                }
                                output.collect(result);
                            }
                        });

        stream1.process(windowProcessFunction).toSink(new WrappedSink<>(new PrintSink<>()));
        env.execute("test process");
    }

    @Test
    void testProcessGlobalWindow() throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getInstance();

        NonKeyedPartitionStream.ProcessConfigurableAndNonKeyedPartitionStream<ValueWithTimestamp>
                source =
                        env.fromSource(
                                new WrappedSource<ValueWithTimestamp>(
                                        new DataGeneratorSource<ValueWithTimestamp>(
                                                new TestGeneratorFunction(),
                                                100_000,
                                                TypeInformation.of(ValueWithTimestamp.class))),
                                "source");

        NonKeyedPartitionStream.ProcessConfigurableAndNonKeyedPartitionStream<ValueWithTimestamp>
                stream1 =
                        source.process(
                                EventTimeExtension.<ValueWithTimestamp>newWatermarkGeneratorBuilder(
                                                element -> element.getTimestamp())
                                        .perEventWatermark()
                                        .buildAsProcessFunction());

        OneInputStreamProcessFunction<ValueWithTimestamp, String> windowProcessFunction =
                BuiltinFuncs.window(
                        WindowStrategy.global(),
                        new OneInputWindowStreamProcessFunction<ValueWithTimestamp, String>() {

                            @Override
                            public void onTrigger(
                                    Collector<String> output,
                                    PartitionedContext<String> ctx,
                                    OneInputWindowContext<ValueWithTimestamp> windowContext)
                                    throws Exception {
                                Iterable<ValueWithTimestamp> allRecords =
                                        windowContext.getAllRecords();
                                String result = "";
                                for (ValueWithTimestamp valueWithTimestamp : allRecords) {
                                    result += valueWithTimestamp.getValue() + ",";
                                }
                                output.collect(result);
                            }
                        });

        stream1.process(windowProcessFunction).toSink(new WrappedSink<>(new PrintSink<>()));
        env.execute("test process");
    }

    @Test
    void testGlobalStreamProcessGlobalWindow() throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getInstance();

        NonKeyedPartitionStream.ProcessConfigurableAndNonKeyedPartitionStream<ValueWithTimestamp>
                source =
                        env.fromSource(
                                new WrappedSource<ValueWithTimestamp>(
                                        new DataGeneratorSource<ValueWithTimestamp>(
                                                new TestGeneratorFunction(),
                                                100_000,
                                                TypeInformation.of(ValueWithTimestamp.class))),
                                "source");

        NonKeyedPartitionStream.ProcessConfigurableAndNonKeyedPartitionStream<ValueWithTimestamp>
                stream1 =
                        source.process(
                                EventTimeExtension.<ValueWithTimestamp>newWatermarkGeneratorBuilder(
                                                element -> element.getTimestamp())
                                        .perEventWatermark()
                                        .buildAsProcessFunction());

        OneInputStreamProcessFunction<ValueWithTimestamp, String> windowProcessFunction =
                BuiltinFuncs.window(
                        WindowStrategy.global(),
                        new OneInputWindowStreamProcessFunction<ValueWithTimestamp, String>() {

                            @Override
                            public void onTrigger(
                                    Collector<String> output,
                                    PartitionedContext<String> ctx,
                                    OneInputWindowContext<ValueWithTimestamp> windowContext)
                                    throws Exception {
                                Iterable<ValueWithTimestamp> allRecords =
                                        windowContext.getAllRecords();
                                String result = "";
                                for (ValueWithTimestamp valueWithTimestamp : allRecords) {
                                    result += valueWithTimestamp.getValue() + ",";
                                }
                                output.collect(result);
                            }
                        });

        stream1.global()
                .process(windowProcessFunction)
                .toSink(new WrappedSink<>(new PrintSink<>()));
        env.execute("test process");
    }

    @Test
    void testGlobalStreamProcessSessionWindow() throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getInstance();

        NonKeyedPartitionStream.ProcessConfigurableAndNonKeyedPartitionStream<ValueWithTimestamp>
                source =
                        env.fromSource(
                                new WrappedSource<ValueWithTimestamp>(
                                        new DataGeneratorSource<ValueWithTimestamp>(
                                                new TestSessionGeneratorFunction(),
                                                100_000,
                                                TypeInformation.of(ValueWithTimestamp.class))),
                                "source");

        NonKeyedPartitionStream.ProcessConfigurableAndNonKeyedPartitionStream<ValueWithTimestamp>
                stream1 =
                        source.process(
                                EventTimeExtension.<ValueWithTimestamp>newWatermarkGeneratorBuilder(
                                                element -> element.getTimestamp())
                                        .perEventWatermark()
                                        .buildAsProcessFunction());

        OneInputStreamProcessFunction<ValueWithTimestamp, String> windowProcessFunction =
                BuiltinFuncs.window(
                        WindowStrategy.session(Duration.ofSeconds(3)),
                        new OneInputWindowStreamProcessFunction<ValueWithTimestamp, String>() {

                            @Override
                            public void onTrigger(
                                    Collector<String> output,
                                    PartitionedContext<String> ctx,
                                    OneInputWindowContext<ValueWithTimestamp> windowContext)
                                    throws Exception {
                                Iterable<ValueWithTimestamp> allRecords =
                                        windowContext.getAllRecords();
                                String result = "";
                                for (ValueWithTimestamp valueWithTimestamp : allRecords) {
                                    result += valueWithTimestamp.getValue() + ",";
                                }
                                output.collect(result);
                            }
                        });

        stream1.global()
                .process(windowProcessFunction)
                .toSink(new WrappedSink<>(new PrintSink<>()));
        env.execute("test process");
    }

    @Test
    void testGlobalStreamProcessTimeWindow() throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getInstance();

        NonKeyedPartitionStream.ProcessConfigurableAndNonKeyedPartitionStream<ValueWithTimestamp>
                source =
                        env.fromSource(
                                new WrappedSource<ValueWithTimestamp>(
                                        new DataGeneratorSource<ValueWithTimestamp>(
                                                new TestGeneratorFunction(),
                                                100_000,
                                                TypeInformation.of(ValueWithTimestamp.class))),
                                "source");

        NonKeyedPartitionStream.ProcessConfigurableAndNonKeyedPartitionStream<ValueWithTimestamp>
                stream1 =
                        source.process(
                                EventTimeExtension.<ValueWithTimestamp>newWatermarkGeneratorBuilder(
                                                element -> element.getTimestamp())
                                        .perEventWatermark()
                                        .buildAsProcessFunction());

        OneInputStreamProcessFunction<ValueWithTimestamp, String> windowProcessFunction =
                BuiltinFuncs.window(
                        WindowStrategy.tumbling(Duration.ofSeconds(5)),
                        new OneInputWindowStreamProcessFunction<ValueWithTimestamp, String>() {

                            @Override
                            public void onTrigger(
                                    Collector<String> output,
                                    PartitionedContext<String> ctx,
                                    OneInputWindowContext<ValueWithTimestamp> windowContext)
                                    throws Exception {
                                Iterable<ValueWithTimestamp> allRecords =
                                        windowContext.getAllRecords();
                                String result = "";
                                for (ValueWithTimestamp valueWithTimestamp : allRecords) {
                                    result += valueWithTimestamp.getValue() + ",";
                                }
                                output.collect(result);
                            }
                        });

        stream1.global()
                .process(windowProcessFunction)
                .toSink(new WrappedSink<>(new PrintSink<>()));
        env.execute("test process");
    }

    @Test
    void testSessionProcess() throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getInstance();

        NonKeyedPartitionStream.ProcessConfigurableAndNonKeyedPartitionStream<ValueWithTimestamp>
                source =
                        env.fromSource(
                                new WrappedSource<ValueWithTimestamp>(
                                        new DataGeneratorSource<ValueWithTimestamp>(
                                                new TestSessionGeneratorFunction(),
                                                100_000,
                                                TypeInformation.of(ValueWithTimestamp.class))),
                                "source");

        NonKeyedPartitionStream.ProcessConfigurableAndNonKeyedPartitionStream<ValueWithTimestamp>
                stream1 =
                        source.process(
                                EventTimeExtension.<ValueWithTimestamp>newWatermarkGeneratorBuilder(
                                                element -> element.getTimestamp())
                                        .perEventWatermark()
                                        .buildAsProcessFunction());

        OneInputStreamProcessFunction<ValueWithTimestamp, String> windowProcessFunction =
                BuiltinFuncs.window(
                        WindowStrategy.session(Duration.ofSeconds(6)),
                        new OneInputWindowStreamProcessFunction<ValueWithTimestamp, String>() {

                            @Override
                            public void onTrigger(
                                    Collector<String> output,
                                    PartitionedContext<String> ctx,
                                    OneInputWindowContext<ValueWithTimestamp> windowContext)
                                    throws Exception {
                                Iterable<ValueWithTimestamp> allRecords =
                                        windowContext.getAllRecords();
                                String result = "";
                                for (ValueWithTimestamp valueWithTimestamp : allRecords) {
                                    result += valueWithTimestamp.getValue() + ",";
                                }
                                output.collect(result);
                            }
                        });

        stream1.process(windowProcessFunction).toSink(new WrappedSink<>(new PrintSink<>()));
        env.execute("test process");
    }

    @Test
    void testTwoInputProcess() throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getInstance();

        NonKeyedPartitionStream.ProcessConfigurableAndNonKeyedPartitionStream<ValueWithTimestamp>
                source1 =
                        env.fromSource(
                                new WrappedSource<ValueWithTimestamp>(
                                        new DataGeneratorSource<ValueWithTimestamp>(
                                                new TestGeneratorFunction(),
                                                100_000,
                                                TypeInformation.of(ValueWithTimestamp.class))),
                                "source");
        NonKeyedPartitionStream.ProcessConfigurableAndNonKeyedPartitionStream<ValueWithTimestamp>
                stream1 =
                        source1.process(
                                EventTimeExtension.<ValueWithTimestamp>newWatermarkGeneratorBuilder(
                                                element -> element.getTimestamp())
                                        .perEventWatermark()
                                        .buildAsProcessFunction());

        NonKeyedPartitionStream.ProcessConfigurableAndNonKeyedPartitionStream<ValueWithTimestamp>
                source2 =
                        env.fromSource(
                                new WrappedSource<ValueWithTimestamp>(
                                        new DataGeneratorSource<ValueWithTimestamp>(
                                                new TestGeneratorFunction(),
                                                100_000,
                                                TypeInformation.of(ValueWithTimestamp.class))),
                                "source");
        NonKeyedPartitionStream.ProcessConfigurableAndNonKeyedPartitionStream<ValueWithTimestamp>
                stream2 =
                        source2.process(
                                EventTimeExtension.<ValueWithTimestamp>newWatermarkGeneratorBuilder(
                                                element -> element.getTimestamp())
                                        .perEventWatermark()
                                        .buildAsProcessFunction());

        TwoInputNonBroadcastStreamProcessFunction<ValueWithTimestamp, ValueWithTimestamp, String>
                twoInputWindowProcessFunction =
                        BuiltinFuncs.window(
                                WindowStrategy.tumbling(Duration.ofSeconds(5)),
                                new TwoInputNonNroadcastWindowStreamProcessFunction<
                                        ValueWithTimestamp, ValueWithTimestamp, String>() {

                                    @Override
                                    public void onTrigger(
                                            Collector<String> output,
                                            PartitionedContext ctx,
                                            TwoInputWindowContext<
                                                            ValueWithTimestamp, ValueWithTimestamp>
                                                    windowContext)
                                            throws Exception {
                                        Iterable<ValueWithTimestamp> allRecords1 =
                                                windowContext.getAllRecords1();
                                        Iterable<ValueWithTimestamp> allRecords2 =
                                                windowContext.getAllRecords2();
                                        String result = "";
                                        for (ValueWithTimestamp valueWithTimestamp : allRecords1) {
                                            result += valueWithTimestamp.getValue() + ",";
                                        }
                                        for (ValueWithTimestamp valueWithTimestamp : allRecords2) {
                                            result += valueWithTimestamp.getValue() + ",";
                                        }
                                        output.collect(result);
                                    }
                                });

        stream1.connectAndProcess(stream2, twoInputWindowProcessFunction)
                .toSink(new WrappedSink<>(new PrintSink<>()));
        env.execute("test process");
    }

    @Test
    void testTwoInputSessionProcess() throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getInstance();

        NonKeyedPartitionStream.ProcessConfigurableAndNonKeyedPartitionStream<ValueWithTimestamp>
                source1 =
                        env.fromSource(
                                new WrappedSource<ValueWithTimestamp>(
                                        new DataGeneratorSource<ValueWithTimestamp>(
                                                new TestSessionGeneratorFunction(),
                                                100_000,
                                                TypeInformation.of(ValueWithTimestamp.class))),
                                "source");
        NonKeyedPartitionStream.ProcessConfigurableAndNonKeyedPartitionStream<ValueWithTimestamp>
                stream1 =
                        source1.process(
                                EventTimeExtension.<ValueWithTimestamp>newWatermarkGeneratorBuilder(
                                                element -> element.getTimestamp())
                                        .perEventWatermark()
                                        .buildAsProcessFunction());

        NonKeyedPartitionStream.ProcessConfigurableAndNonKeyedPartitionStream<ValueWithTimestamp>
                source2 =
                        env.fromSource(
                                new WrappedSource<ValueWithTimestamp>(
                                        new DataGeneratorSource<ValueWithTimestamp>(
                                                new TestSessionGeneratorFunction(),
                                                100_000,
                                                TypeInformation.of(ValueWithTimestamp.class))),
                                "source");
        NonKeyedPartitionStream.ProcessConfigurableAndNonKeyedPartitionStream<ValueWithTimestamp>
                stream2 =
                        source2.process(
                                EventTimeExtension.<ValueWithTimestamp>newWatermarkGeneratorBuilder(
                                                element -> element.getTimestamp())
                                        .perEventWatermark()
                                        .buildAsProcessFunction());

        TwoInputNonBroadcastStreamProcessFunction<ValueWithTimestamp, ValueWithTimestamp, String>
                twoInputWindowProcessFunction =
                        BuiltinFuncs.window(
                                WindowStrategy.session(Duration.ofSeconds(5)),
                                new TwoInputNonNroadcastWindowStreamProcessFunction<
                                        ValueWithTimestamp, ValueWithTimestamp, String>() {

                                    @Override
                                    public void onTrigger(
                                            Collector<String> output,
                                            PartitionedContext ctx,
                                            TwoInputWindowContext<
                                                            ValueWithTimestamp, ValueWithTimestamp>
                                                    windowContext)
                                            throws Exception {
                                        Iterable<ValueWithTimestamp> allRecords1 =
                                                windowContext.getAllRecords1();
                                        Iterable<ValueWithTimestamp> allRecords2 =
                                                windowContext.getAllRecords2();
                                        String result = "";
                                        for (ValueWithTimestamp valueWithTimestamp : allRecords1) {
                                            result += valueWithTimestamp.getValue() + ",";
                                        }
                                        for (ValueWithTimestamp valueWithTimestamp : allRecords2) {
                                            result += valueWithTimestamp.getValue() + ",";
                                        }
                                        output.collect(result);
                                    }
                                });

        stream1.connectAndProcess(stream2, twoInputWindowProcessFunction)
                .toSink(new WrappedSink<>(new PrintSink<>()));
        env.execute("test process");
    }

    @Test
    void testTwoOutputNonKeyed() throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getInstance();

        NonKeyedPartitionStream.ProcessConfigurableAndNonKeyedPartitionStream<ValueWithTimestamp>
                source =
                        env.fromSource(
                                new WrappedSource<ValueWithTimestamp>(
                                        new DataGeneratorSource<ValueWithTimestamp>(
                                                new TestGeneratorFunction(),
                                                100_000,
                                                TypeInformation.of(ValueWithTimestamp.class))),
                                "source");

        NonKeyedPartitionStream.ProcessConfigurableAndNonKeyedPartitionStream<ValueWithTimestamp>
                stream1 =
                        source.process(
                                EventTimeExtension.<ValueWithTimestamp>newWatermarkGeneratorBuilder(
                                                element -> element.getTimestamp())
                                        .perEventWatermark()
                                        .buildAsProcessFunction());

        TwoOutputStreamProcessFunction<ValueWithTimestamp, String, String> windowProcessFunction =
                BuiltinFuncs.window(
                        WindowStrategy.tumbling(Duration.ofSeconds(5)),
                        new TwoOutputWindowStreamProcessFunction<
                                ValueWithTimestamp, String, String>() {

                            @Override
                            public void onTrigger(
                                    Collector<String> output1,
                                    Collector<String> output2,
                                    TwoOutputPartitionedContext ctx,
                                    OneInputWindowContext<ValueWithTimestamp> windowContext)
                                    throws Exception {
                                Iterable<ValueWithTimestamp> allRecords =
                                        windowContext.getAllRecords();
                                String result = "";
                                for (ValueWithTimestamp valueWithTimestamp : allRecords) {
                                    result += valueWithTimestamp.getValue() + ",";
                                }
                                output1.collect(result);
                                output2.collect(result);
                            }
                        });

        NonKeyedPartitionStream.ProcessConfigurableAndTwoNonKeyedPartitionStream<String, String>
                twoOutputStream = stream1.process(windowProcessFunction);
        twoOutputStream
                .getFirst()
                .process(
                        new OneInputStreamProcessFunction<String, Object>() {
                            @Override
                            public void processRecord(
                                    String record,
                                    Collector<Object> output,
                                    PartitionedContext<Object> ctx)
                                    throws Exception {
                                System.out.println("FirstInput record: " + record);
                            }
                        });

        twoOutputStream
                .getSecond()
                .process(
                        new OneInputStreamProcessFunction<String, Object>() {
                            @Override
                            public void processRecord(
                                    String record,
                                    Collector<Object> output,
                                    PartitionedContext<Object> ctx)
                                    throws Exception {
                                System.out.println("SecondInput record: " + record);
                            }
                        });
        env.execute("test process");
    }

    @Test
    void testTwoOutputGlobalStream() throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getInstance();

        NonKeyedPartitionStream.ProcessConfigurableAndNonKeyedPartitionStream<ValueWithTimestamp>
                source =
                        env.fromSource(
                                new WrappedSource<ValueWithTimestamp>(
                                        new DataGeneratorSource<ValueWithTimestamp>(
                                                new TestGeneratorFunction(),
                                                100_000,
                                                TypeInformation.of(ValueWithTimestamp.class))),
                                "source");

        NonKeyedPartitionStream.ProcessConfigurableAndNonKeyedPartitionStream<ValueWithTimestamp>
                stream1 =
                        source.process(
                                EventTimeExtension.<ValueWithTimestamp>newWatermarkGeneratorBuilder(
                                                element -> element.getTimestamp())
                                        .perEventWatermark()
                                        .buildAsProcessFunction());

        TwoOutputStreamProcessFunction<ValueWithTimestamp, String, String> windowProcessFunction =
                BuiltinFuncs.window(
                        WindowStrategy.tumbling(Duration.ofSeconds(5)),
                        new TwoOutputWindowStreamProcessFunction<
                                ValueWithTimestamp, String, String>() {

                            @Override
                            public void onTrigger(
                                    Collector<String> output1,
                                    Collector<String> output2,
                                    TwoOutputPartitionedContext ctx,
                                    OneInputWindowContext<ValueWithTimestamp> windowContext)
                                    throws Exception {
                                Iterable<ValueWithTimestamp> allRecords =
                                        windowContext.getAllRecords();
                                String result = "";
                                for (ValueWithTimestamp valueWithTimestamp : allRecords) {
                                    result += valueWithTimestamp.getValue() + ",";
                                }
                                output1.collect(result);
                                output2.collect(result);
                            }
                        });

        GlobalStream.TwoGlobalStreams<String, String> twoOutputStream =
                stream1.global().process(windowProcessFunction);
        twoOutputStream
                .getFirst()
                .process(
                        new OneInputStreamProcessFunction<String, Object>() {
                            @Override
                            public void processRecord(
                                    String record,
                                    Collector<Object> output,
                                    PartitionedContext<Object> ctx)
                                    throws Exception {
                                System.out.println("FirstInput record: " + record);
                            }
                        });

        twoOutputStream
                .getSecond()
                .process(
                        new OneInputStreamProcessFunction<String, Object>() {
                            @Override
                            public void processRecord(
                                    String record,
                                    Collector<Object> output,
                                    PartitionedContext<Object> ctx)
                                    throws Exception {
                                System.out.println("SecondInput record: " + record);
                            }
                        });
        env.execute("test process");
    }

    //
    //    //    @Test
    //    //    void testNonKeyed() throws Exception {
    //    //        ExecutionEnvironment env = ExecutionEnvironment.getInstance();
    //    //
    //    //
    // NonKeyedPartitionStream.ProcessConfigurableAndNonKeyedPartitionStream<ValueWithTimestamp>
    //    //                source =
    //    //                        env.fromSource(
    //    //                                new WrappedSource<ValueWithTimestamp>(
    //    //                                        new DataGeneratorSource<ValueWithTimestamp>(
    //    //                                                new TestGeneratorFunction(),
    //    //                                                100_000,
    //    //
    //    // TypeInformation.of(ValueWithTimestamp.class))),
    //    //                                "source");
    //    //
    //    //        NonKeyedPartitionStream.ProcessConfigurableAndNonKeyedPartitionStream<Long>
    // process =
    //    //                source.process(EventTimeExtension.extractEventTime(element ->
    //    // element.timestamp))
    //    //                        .process(
    //    //                                new OneInputStreamProcessFunction<ValueWithTimestamp,
    // Long>()
    //    // {
    //    //                                    @Override
    //    //                                    public void processRecord(
    //    //                                            ValueWithTimestamp record,
    //    //                                            Collector<Long> output,
    //    //                                            PartitionedContext ctx)
    //    //                                            throws Exception {
    //    //                                        output.collect((long) record.getValue());
    //    //                                    }
    //    //                                })
    //    //                        .process(
    //    //                                WindowExtension.apply(
    //    //                                        WindowExtension.TimeWindows.ofTumbling(
    //    //                                                Duration.ofSeconds(5),
    //    //
    // WindowExtension.TimeWindows.TimeType.EVENT),
    //    //                                        new ReduceFunction<Long>() {
    //    //                                            @Override
    //    //                                            public Long reduce(Long value1, Long value2)
    //    //                                                    throws Exception {
    //    //                                                return value1 + value2;
    //    //                                            }
    //    //                                        },
    //    //                                        new WindowProcessFunction<Long, Long,
    // TimeWindow>() {
    //    //                                            @Override
    //    //                                            public void onWindowTrigger(
    //    //                                                    Long record,
    //    //                                                    Collector<Long> output,
    //    //                                                    PartitionedContext ctx,
    //    //                                                    WindowContext<TimeWindow>
    // windowContext)
    //    //                                                    throws Exception {
    //    //                                                output.collect(record);
    //    //                                            }
    //    //                                        }));
    //    //        process.toSink(new WrappedSink<>(new PrintSink<>()));
    //    //        env.execute("test non keyed");
    //    //    }
    //
    //    //    @Test
    //    //    void testGlobal() throws Exception {
    //    //
    //    //        ExecutionEnvironment env = ExecutionEnvironment.getInstance();
    //    //
    //    //
    // NonKeyedPartitionStream.ProcessConfigurableAndNonKeyedPartitionStream<ValueWithTimestamp>
    //    //                source =
    //    //                        env.fromSource(
    //    //                                new WrappedSource<ValueWithTimestamp>(
    //    //                                        new DataGeneratorSource<ValueWithTimestamp>(
    //    //                                                new TestGeneratorFunction(),
    //    //                                                100_000,
    //    //
    //    // TypeInformation.of(ValueWithTimestamp.class))),
    //    //                                "source");
    //    //
    //    //        GlobalStream.ProcessConfigurableAndGlobalStream<Long> process =
    //    //                source.process(EventTimeExtension.extractEventTime(element ->
    //    // element.timestamp))
    //    //                        .process(
    //    //                                new OneInputStreamProcessFunction<ValueWithTimestamp,
    // Long>()
    //    // {
    //    //                                    @Override
    //    //                                    public void processRecord(
    //    //                                            ValueWithTimestamp record,
    //    //                                            Collector<Long> output,
    //    //                                            PartitionedContext ctx)
    //    //                                            throws Exception {
    //    //                                        output.collect((long) record.getValue());
    //    //                                    }
    //    //                                })
    //    //                        .withParallelism(2)
    //    //                        .global()
    //    //                        .process(
    //    //                                WindowExtension.apply(
    //    //                                        WindowExtension.TimeWindows.ofTumbling(
    //    //                                                Duration.ofSeconds(5),
    //    //
    // WindowExtension.TimeWindows.TimeType.EVENT),
    //    //                                        new ReduceFunction<Long>() {
    //    //                                            @Override
    //    //                                            public Long reduce(Long value1, Long value2)
    //    //                                                    throws Exception {
    //    //                                                return value1 + value2;
    //    //                                            }
    //    //                                        },
    //    //                                        new WindowProcessFunction<Long, Long,
    // TimeWindow>() {
    //    //                                            @Override
    //    //                                            public void onWindowTrigger(
    //    //                                                    Long record,
    //    //                                                    Collector<Long> output,
    //    //                                                    PartitionedContext ctx,
    //    //                                                    WindowContext<TimeWindow>
    // windowContext)
    //    //                                                    throws Exception {
    //    //                                                output.collect(record);
    //    //                                            }
    //    //                                        }));
    //    //        process.toSink(new WrappedSink<>(new PrintSink<>()));
    //    //        env.execute("test global");
    //    //    }
    //
    //    private static class PerElementWatermarkGenerator implements WatermarkGenerator<Long> {
    //
    //        private long maxTimestamp;
    //
    //        public PerElementWatermarkGenerator() {
    //            maxTimestamp = Long.MIN_VALUE;
    //        }
    //
    //        @Override
    //        public void onEvent(Long event, long eventTimestamp, WatermarkOutput output) {
    //            if (eventTimestamp > maxTimestamp) {
    //                maxTimestamp = eventTimestamp;
    //                output.emitWatermark(new Watermark(eventTimestamp));
    //            }
    //        }
    //
    //        @Override
    //        public void onPeriodicEmit(WatermarkOutput output) {}
    //
    //        public static WatermarkGeneratorSupplier<Long> getSupplier() {
    //            return (ctx) -> new PerElementWatermarkGenerator();
    //        }
    //    }
    //
    //    private static class ElementValueTimestampAssigner implements TimestampAssigner<Long> {
    //
    //        @Override
    //        public long extractTimestamp(Long element, long recordTimestamp) {
    //            return element;
    //        }
    //    }
    //
    //    public static class PerElementValueWithTimestampWatermarkGenerator
    //            implements WatermarkGenerator<ValueWithTimestamp> {
    //
    //        private long maxTimestamp;
    //
    //        public PerElementValueWithTimestampWatermarkGenerator() {
    //            maxTimestamp = Long.MIN_VALUE;
    //        }
    //
    //        @Override
    //        public void onEvent(ValueWithTimestamp event, long eventTimestamp, WatermarkOutput
    // output) {
    //            if (eventTimestamp > maxTimestamp) {
    //                maxTimestamp = eventTimestamp;
    //                output.emitWatermark(new Watermark(eventTimestamp));
    //            }
    //        }
    //
    //        @Override
    //        public void onPeriodicEmit(WatermarkOutput output) {}
    //
    //        public static WatermarkGeneratorSupplier<ValueWithTimestamp> getSupplier() {
    //            return (ctx) -> new PerElementValueWithTimestampWatermarkGenerator();
    //        }
    //    }
    //

    public static class ValueWithTimestamp {
        private final long timestamp;

        private final int value;

        public ValueWithTimestamp(long timestamp, int value) {
            this.timestamp = timestamp;
            this.value = value;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public int getValue() {
            return value;
        }

        @Override
        public String toString() {
            return "ValueWithTimestamp{" + "timestamp=" + timestamp + ", value=" + value + '}';
        }
    }

    public static class TestGeneratorFunction
            implements GeneratorFunction<Long, ValueWithTimestamp> {

        private long curTime = 1689847907000L;

        private int elementValue = 0;

        @Override
        public ValueWithTimestamp map(Long value) throws Exception {
            curTime = curTime + Duration.ofSeconds(1).toMillis();
            return new ValueWithTimestamp(curTime, elementValue++);
        }
    }

    public static class TestSessionGeneratorFunction
            implements GeneratorFunction<Long, ValueWithTimestamp> {

        private long curTime = 1689847907000L;

        private int elementValue = 0;

        private int count = 0;

        @Override
        public ValueWithTimestamp map(Long value) throws Exception {
            if (count % 5 == 0) {
                curTime += Duration.ofSeconds(5).toMillis();
            }
            curTime = curTime + Duration.ofSeconds(1).toMillis();
            count++;
            return new ValueWithTimestamp(curTime, elementValue++);
        }
    }
}
