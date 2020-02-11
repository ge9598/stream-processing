package clusterdata.utils;

import clusterdata.datatypes.EventType;
import clusterdata.datatypes.TaskEvent;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParseException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

/**
 * Implements a SerializationSchema and DeserializationSchema for TaskEvent for Kafka data sources and sinks.
 */
public class TaskEventSchema implements DeserializationSchema<TaskEvent>, SerializationSchema<TaskEvent>  {
    ObjectMapper ob;
    byte[] b = new byte[0];
    @Override
    public byte[] serialize(TaskEvent element) {
        //TODO: implement this method
        if(ob == null) {
            ob = new ObjectMapper();
        }
        try{
            b = ob.writeValueAsString(element).getBytes();
        }catch (JsonProcessingException e){
            e.printStackTrace();
        }
        return b;
    }

    @Override
    public TaskEvent deserialize(byte[] message) {
        //TODO: implement this method
        TaskEvent te = new TaskEvent();
        try {
            te =  ob.readValue(message, TaskEvent.class);
        }catch (IOException e){
            e.printStackTrace();
        }
        return te;
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
