package clusterdata.utils;

import clusterdata.datatypes.TaskEvent;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

public class TupleSchema implements DeserializationSchema<Tuple3<TaskEvent,TaskEvent, TaskEvent>>, SerializationSchema<Tuple3<TaskEvent,TaskEvent, TaskEvent>> {
    ObjectMapper ob;
    byte[] b = new byte[0];
    @Override
    public Tuple3 deserialize(byte[] bytes) throws IOException {
        if (ob == null) {
            ob = new ObjectMapper();
        }
        //read the serialized message and return the actual message
        return ob.readValue(bytes, Tuple3.class);
    }


    @Override
    public byte[] serialize(Tuple3 tuple3) {
        if(ob == null) {
            ob = new ObjectMapper();
        }
        try{
            b = ob.writeValueAsString(tuple3).getBytes();
        }catch (JsonProcessingException e){
            e.printStackTrace();
        }
        return b;
    }

    @Override
    public TypeInformation<Tuple3<TaskEvent,TaskEvent, TaskEvent>> getProducedType() {
        return null;
    }
    @Override
    public boolean isEndOfStream(Tuple3 tuple3) {
        return false;
    }

}
