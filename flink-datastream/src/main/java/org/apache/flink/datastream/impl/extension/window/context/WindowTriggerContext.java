package org.apache.flink.datastream.impl.extension.window.context;

import org.apache.flink.api.common.state.MergingState;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.Collection;

/**
 * {@code Context} is a utility for handling {@code Trigger} invocations. It can be reused by
 * setting the {@code key} and {@code window} fields. No internal state must be kept in the {@code
 * Context}
 */
public class WindowTriggerContext<K, IN, W extends Window> implements Trigger.OnMergeContext {
    private K key;
    private W window;
    private final AbstractStreamOperator<?> operator;
    private final transient InternalTimerService<W> internalTimerService;
    private Collection<W> mergedWindows;
    private final Trigger<? super IN, ? super W> trigger;
    private final TypeSerializer<W> windowSerializer;

    public WindowTriggerContext(
            K key,
            W window,
            AbstractStreamOperator<?> operator,
            InternalTimerService<W> internalTimerService,
            Trigger<? super IN, ? super W> trigger,
            TypeSerializer<W> windowSerializer) {
        this.key = key;
        this.window = window;
        this.operator = operator;
        this.internalTimerService = internalTimerService;
        this.trigger = trigger;
        this.windowSerializer = windowSerializer;
    }

    @Override
    public MetricGroup getMetricGroup() {
        return operator.getMetricGroup();
    }

    public long getCurrentWatermark() {
        return internalTimerService.currentWatermark();
    }

    @SuppressWarnings("unchecked")
    public <S extends State> S getPartitionedState(
            org.apache.flink.api.common.state.StateDescriptor<S, ?> stateDescriptor) {
        try {
            return operator.getPartitionedState(window, windowSerializer, stateDescriptor);
        } catch (Exception e) {
            throw new RuntimeException("Could not retrieve state", e);
        }
    }

    @Override
    public <S extends MergingState<?, ?>> void mergePartitionedState(
            org.apache.flink.api.common.state.StateDescriptor<S, ?> stateDescriptor) {
        if (mergedWindows != null && mergedWindows.size() > 0) {
            try {
                S rawState =
                        operator.getKeyedStateBackend()
                                .getOrCreateKeyedState(windowSerializer, stateDescriptor);

                if (rawState
                        instanceof org.apache.flink.runtime.state.internal.InternalMergingState) {
                    @SuppressWarnings("unchecked")
                    org.apache.flink.runtime.state.internal.InternalMergingState<K, W, ?, ?, ?>
                            mergingState =
                                    (org.apache.flink.runtime.state.internal.InternalMergingState<
                                                    K, W, ?, ?, ?>)
                                            rawState;
                    mergingState.mergeNamespaces(window, mergedWindows);
                } else {
                    throw new IllegalArgumentException(
                            "The given state descriptor does not refer to a mergeable state (MergingState)");
                }
            } catch (Exception e) {
                throw new RuntimeException("Error while merging state.", e);
            }
        }
    }

    @Override
    public long getCurrentProcessingTime() {
        return internalTimerService.currentProcessingTime();
    }

    @Override
    public void registerProcessingTimeTimer(long time) {
        internalTimerService.registerProcessingTimeTimer(window, time);
    }

    @Override
    public void registerEventTimeTimer(long time) {
        internalTimerService.registerEventTimeTimer(window, time);
    }

    @Override
    public void deleteProcessingTimeTimer(long time) {
        internalTimerService.deleteProcessingTimeTimer(window, time);
    }

    @Override
    public void deleteEventTimeTimer(long time) {
        internalTimerService.deleteEventTimeTimer(window, time);
    }

    public TriggerResult onElement(StreamRecord<IN> element) throws Exception {
        return trigger.onElement(element.getValue(), element.getTimestamp(), window, this);
    }

    public TriggerResult onProcessingTime(long time) throws Exception {
        return trigger.onProcessingTime(time, window, this);
    }

    public TriggerResult onEventTime(long time) throws Exception {
        return trigger.onEventTime(time, window, this);
    }

    public void onMerge(Collection<W> mergedWindows) throws Exception {
        this.mergedWindows = mergedWindows;
        trigger.onMerge(window, this);
    }

    public void clear() throws Exception {
        trigger.clear(window, this);
    }

    @Override
    public String toString() {
        return "WindowTriggerContext{" + "key=" + key + ", window=" + window + '}';
    }

    public void setKey(K key) {
        this.key = key;
    }

    public void setWindow(W window) {
        this.window = window;
    }

    public K getKey() {
        return key;
    }

    public W getWindow() {
        return window;
    }
}
