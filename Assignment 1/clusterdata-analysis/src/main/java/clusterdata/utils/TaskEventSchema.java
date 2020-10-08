package clusterdata.utils;

import clusterdata.datatypes.EventType;
import clusterdata.datatypes.TaskEvent;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;

/**
 * Implements a SerializationSchema and DeserializationSchema for TaskEvent for Kafka data sources and sinks.
 */
public class TaskEventSchema implements DeserializationSchema<TaskEvent>, SerializationSchema<TaskEvent>  {

    @Override
    public byte[] serialize(TaskEvent element) {
        //TODO: implement this method
        return null;
    }

    @Override
    public TaskEvent deserialize(byte[] message) {
        //TODO: implement this method
        return null;
    }

    @Override
    public boolean isEndOfStream(TaskEvent nextElement) {
        return false;
    }

    @Override
    public TypeInformation<TaskEvent> getProducedType() {
        return TypeExtractor.getForClass(TaskEvent.class);
    }
}
