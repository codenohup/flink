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

package org.apache.flink.datastream.impl.extension.window.function;

import org.apache.flink.api.common.state.StateDeclaration;
import org.apache.flink.datastream.api.common.Collector;
import org.apache.flink.datastream.api.context.TwoOutputPartitionedContext;
import org.apache.flink.datastream.api.extension.window.function.TwoOutputWindowStreamProcessFunction;
import org.apache.flink.datastream.api.function.TwoOutputStreamProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.Window;

import java.util.Set;

/** A class that wrap a {@link TwoOutputWindowStreamProcessFunction} to internal processing. */
public class InternalTwoOutputWindowStreamProcessFunction<IN, OUT1, OUT2, W extends Window>
        implements TwoOutputStreamProcessFunction<IN, OUT1, OUT2> {

    private final TwoOutputWindowStreamProcessFunction<IN, OUT1, OUT2> windowProcessFunction;

    private final WindowAssigner<IN, W> assigner;

    private final Trigger<IN, W> trigger;

    public InternalTwoOutputWindowStreamProcessFunction(
            TwoOutputWindowStreamProcessFunction<IN, OUT1, OUT2> windowProcessFunction,
            WindowAssigner<IN, W> assigner,
            Trigger<IN, W> trigger) {
        this.windowProcessFunction = windowProcessFunction;
        this.assigner = assigner;
        this.trigger = trigger;
    }

    @Override
    public void processRecord(
            IN record,
            Collector<OUT1> output1,
            Collector<OUT2> output2,
            TwoOutputPartitionedContext<OUT1, OUT2> ctx)
            throws Exception {
        // Do nothing as this will translator to windowOperator instead of processOperator.
    }

    public WindowAssigner<IN, W> getAssigner() {
        return assigner;
    }

    public Trigger<IN, W> getTrigger() {
        return trigger;
    }

    public TwoOutputWindowStreamProcessFunction<IN, OUT1, OUT2> getWindowProcessFunction() {
        return windowProcessFunction;
    }

    @Override
    public Set<StateDeclaration> usesStates() {
        return windowProcessFunction.usesStates();
    }
}
