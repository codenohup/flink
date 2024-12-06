package org.apache.flink.datastream.api.stream;

import java.io.Serializable;

public interface EventTimeExtractor<T> extends Serializable {

    /** should be in millisecond. */
    long extractTimestamp(T element);
}
