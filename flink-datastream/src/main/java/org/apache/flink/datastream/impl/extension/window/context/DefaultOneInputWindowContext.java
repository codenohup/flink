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

package org.apache.flink.datastream.impl.extension.window.context;

import org.apache.flink.api.common.state.ListStateDeclaration;
import org.apache.flink.api.common.state.MapStateDeclaration;
import org.apache.flink.api.common.state.ValueStateDeclaration;
import org.apache.flink.api.common.state.v2.AppendingState;
import org.apache.flink.api.common.state.v2.ListState;
import org.apache.flink.api.common.state.v2.MapState;
import org.apache.flink.api.common.state.v2.StateIterator;
import org.apache.flink.api.common.state.v2.ValueState;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.datastream.api.extension.window.context.OneInputWindowContext;
import org.apache.flink.datastream.api.extension.window.function.WindowProcessFunction;
import org.apache.flink.datastream.impl.extension.window.utils.WindowStateStore;
import org.apache.flink.runtime.asyncprocessing.operators.AbstractAsyncStateStreamOperator;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;

import java.util.Optional;

public class DefaultOneInputWindowContext<K, IN, W extends Window>
        implements OneInputWindowContext<IN> {
    private W window;
    private WindowStateStore<K, W> windowStateStore;
    private AppendingState<IN, StateIterator<IN>, Iterable<IN>> windowState;

    public DefaultOneInputWindowContext(
            W window,
            AppendingState<IN, StateIterator<IN>, Iterable<IN>> windowState,
            WindowProcessFunction windowProcessFunction,
            AbstractAsyncStateStreamOperator<?> operator,
            TypeSerializer<W> windowSerializer) {
        this.window = window;
        this.windowState = windowState;
        this.windowStateStore =
                new WindowStateStore<>(windowProcessFunction, operator, windowSerializer);
    }

    public void setWindow(W window) {
        this.window = window;
    }

    @Override
    public long getStartTime() {
        if (window instanceof TimeWindow) {
            return ((TimeWindow) window).getStart();
        }
        return -1;
    }

    @Override
    public long getEndTime() {
        if (window instanceof TimeWindow) {
            return ((TimeWindow) window).getEnd();
        }
        return -1;
    }

    @Override
    public <T> Optional<ListState<T>> getWindowState(ListStateDeclaration<T> stateDeclaration)
            throws Exception {
        return windowStateStore.getWindowState(stateDeclaration, window);
    }

    @Override
    public <KEY, V> Optional<MapState<KEY, V>> getWindowState(
            MapStateDeclaration<KEY, V> stateDeclaration) throws Exception {
        return windowStateStore.getWindowState(stateDeclaration, window);
    }

    @Override
    public <T> Optional<ValueState<T>> getWindowState(ValueStateDeclaration<T> stateDeclaration)
            throws Exception {
        return windowStateStore.getWindowState(stateDeclaration, window);
    }

    @Override
    public void putRecord(IN record) {
        windowState.add(record);
    }

    @Override
    public Iterable<IN> getAllRecords() {
        return windowState.get();
    }
}
