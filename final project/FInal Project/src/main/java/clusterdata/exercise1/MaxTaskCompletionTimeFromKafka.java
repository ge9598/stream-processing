package clusterdata.exercise1;

import clusterdata.datatypes.JobEvent;
import clusterdata.datatypes.TaskEvent;
import clusterdata.utils.AppBase;
import clusterdata.utils.TaskEventSchema;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Properties;

/**
 * This programs reads the filtered TaskEvents back from Kafka and computes the maximum task duration per priority.
 *
 * Parameters:
 * --input path-to-input-file
 */
public class MaxTaskCompletionTimeFromKafka extends AppBase {

    private static final String LOCAL_ZOOKEEPER_HOST = "localhost:2181";
    private static final String LOCAL_KAFKA_BROKER = "localhost:9092";
    private static final String TASKS_GROUP = "taskGroup";

    public static void main(String[] args) throws Exception {

        // set up streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // do not worry about the following two configuration options for now
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(1000);

        // set the parallelism
        // TODO: check that your program works correctly with higher parallelism
        env.setParallelism(1);

        //TODO: configure the Kafka consumer
        // always read the Kafka topic from the start
        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("zookeeper.connect", LOCAL_ZOOKEEPER_HOST);
        kafkaProps.setProperty("bootstrap.servers", LOCAL_KAFKA_BROKER);
        kafkaProps.setProperty("auto.offset.reset", "earliest");
        kafkaProps.setProperty("group.id", TASKS_GROUP);

        //TODO: implement the following transformations
        // create a Kafka consumer
        // FlinkKafkaConsumer011<TaskEvent> consumer = ...
        // assign a timestamp extractor to the consumer (provided below)
        // consumer.assignTimestampsAndWatermarks(new TSExtractor());
        // create the data stream
        // DataStream<TaskEvent> events = env.addSource(taskSourceOrTest(consumer));
        // compute the durations per task
        // DataStream<Tuple2<Integer, Long>> taskDurations = events...
        // output the maximum duration per priority in 2-tuples of (priority, duration)
        // DataStream<Tuple2<Integer, Long>> maxDurationsPerPriority = taskDurations
        // printOrTest(maxDurationsPerPriority);

        FlinkKafkaConsumer011<TaskEvent> consumer = new FlinkKafkaConsumer011<>(
                FilterTaskEventsToKafka.FILTERED_TASKS_TOPIC,
                new TaskEventSchema(),//deserialization schema
                kafkaProps);
        consumer.assignTimestampsAndWatermarks(new TSExtractor());

        //create datastream
        DataStream<TaskEvent> events = env.addSource(taskSourceOrTest(consumer));

        //filter the events to only submit and finish
        DataStream<TaskEvent> filerEvents = events.filter(new FilterFunction<TaskEvent>() {
            @Override
            public boolean filter(TaskEvent taskEvent) throws Exception {
                if(taskEvent.eventType.getValue() == 0 || taskEvent.eventType.getValue() == 4){
                    return true;
                }else{
                    return false;
                }
            }
        });

        //group the event by jobid and taskindex
        DataStream<TaskEvent> keyedEvent = filerEvents.keyBy("jobId", "taskIndex");
        // use flatmap and in memory hashmap to store priority as key and tuple 2 which contains task event type and timestamp

        DataStream<Tuple2<Integer, Long>> taskDurations = keyedEvent.flatMap(new RichFlatMapFunction<TaskEvent, Tuple2<Integer, Long>>() {
            private MapState<Integer, TaskEvent> taskEventState;//priority, elapse
            HashMap<Integer, Tuple2<Long, Integer>> duration = new HashMap<>();
            @Override
            public void flatMap(TaskEvent taskEvent, Collector<Tuple2<Integer, Long>> collector) throws Exception {
                //put in the hashmap if there is no key
                if(!taskEventState.contains(taskEvent.priority)){
                    taskEventState.put(taskEvent.priority, taskEvent);
                }else{
                    for (TaskEvent curr: taskEventState.values()){
                        if(curr.priority == taskEvent.priority){
                            if(curr.eventType.getValue() == 0 && taskEvent.eventType.getValue() == 4){
                                long elaspe = taskEvent.timestamp - curr.timestamp;
                                collector.collect(new Tuple2<>(taskEvent.priority, elaspe));
                            }
                            else if(curr.eventType.getValue() == 4 && taskEvent.eventType.getValue() == 0){
                                long elaspe =  curr.timestamp - taskEvent.timestamp;
                                collector.collect(new Tuple2<>(taskEvent.priority, elaspe));
                            }
                        }
                    }

                }

                if(!duration.containsKey(taskEvent.priority)) {
                    duration.put(taskEvent.priority,new Tuple2<Long, Integer>(taskEvent.timestamp, taskEvent.eventType.getValue()));
                }else{

                    if(taskEvent.eventType.getValue() == 4 && duration.get(taskEvent.priority).f1 == 0) {
                        long elaspe = taskEvent.timestamp - duration.remove(taskEvent.priority).f0;
                        collector.collect(new Tuple2<>(taskEvent.priority, elaspe));
                    }
                    else if(taskEvent.eventType.getValue() == 0 && duration.get(taskEvent.priority).f1 == 4) {
                        long elaspe =  duration.remove(taskEvent.priority).f0 - taskEvent.timestamp ;
                        collector.collect(new Tuple2<>(taskEvent.priority, elaspe));
                    }

                }
            }

            @Override
            public void open(Configuration parameters) throws Exception {
                taskEventState = getRuntimeContext().getMapState(new MapStateDescriptor<>("maxTask",Integer.class,TaskEvent.class));
            }
        });
        DataStream<Tuple2<Integer, Long>> maxDurationsPerPriority = taskDurations.keyBy(0).maxBy(1);
        printOrTest(maxDurationsPerPriority);
        env.execute();
    }

    /**
     * Assigns timestamps to TaskEvent records.
     * Watermarks are a fixed time interval behind the max timestamp and are periodically emitted.
     */
    public static class TSExtractor extends BoundedOutOfOrdernessTimestampExtractor<TaskEvent> {

        public TSExtractor() {
            super(Time.seconds(0));
        }

        @Override
        public long extractTimestamp(TaskEvent event) {
           return event.timestamp;
        }
    }
}
