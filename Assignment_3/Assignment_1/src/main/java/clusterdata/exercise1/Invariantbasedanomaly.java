package clusterdata.exercise1;

import clusterdata.datatypes.TaskEvent;
import clusterdata.sources.TaskEventSource;
import clusterdata.utils.AppBase;
import clusterdata.utils.TaskEventSchema;
import org.apache.commons.cli.*;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

import static clusterdata.utils.AppBase.*;

public class Invariantbasedanomaly {
    static int te;//training end
    static final int MIN = 1;
    static final int MAX = 2;
    private static final String LOCAL_KAFKA_BROKER = "localhost:9092";
    public static final String FILTERED_TASKS_TOPIC = "filteredTasks";
    private static final String LOCAL_ZOOKEEPER_HOST = "localhost:2181";
    private static final String TASKS_GROUP = "taskGroup";
    public static void main(String[] args) throws Exception {
        Invariantbasedanomaly iba = new Invariantbasedanomaly();
        iba.init(args);
        ParameterTool params = ParameterTool.fromArgs(args);
        final String input = params.get("input", AppBase.pathToTaskEventData);
        final int servingSpeedFactor = 600;
        final int maxEventDelaySecs = 100;
        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("zookeeper.connect", LOCAL_ZOOKEEPER_HOST);
        kafkaProps.setProperty("bootstrap.servers", LOCAL_KAFKA_BROKER);
        kafkaProps.setProperty("auto.offset.reset", "earliest");
        kafkaProps.setProperty("group.id", TASKS_GROUP);
        // set up streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//        env.getConfig().setAutoWatermarkInterval(1000);



        // set the parallelism
        // TODO: check that your program works correctly with higher parallelism
        env.setParallelism(2);
//        env.getConfig().setLatencyTrackingInterval(500);
        // get source data
        FlinkKafkaConsumer011<TaskEvent> consumer = new FlinkKafkaConsumer011<>(
                FilterTaskEventsToKafka.FILTERED_TASKS_TOPIC,
                new TaskEventSchema(),//deserialization schema
                kafkaProps);
        consumer.assignTimestampsAndWatermarks(new MaxTaskCompletionTimeFromKafka.TSExtractor());
        DataStream<TaskEvent> events = env.addSource(taskSourceOrTest(consumer));
//
//        DataStream<TaskEvent> taskEvents = env
//                .addSource(taskSourceOrTest(new TaskEventSource(input, maxEventDelaySecs, servingSpeedFactor)))
//                //compute when its safe to compute, based on the watermark
//                .setParallelism(1);
        //filter jobs
        DataStream<TaskEvent> filterTaskEvents = events.filter(new FilterFunction<TaskEvent>() {
            @Override
            public boolean filter(TaskEvent taskEvent) throws Exception {//filter submit and schedule
                if(taskEvent.eventType.getValue() == 0 || taskEvent.eventType.getValue() == 1){
                    return true;
                }
                else {
                    return false;
                }
            }
        });
        DataStream<Tuple2<Integer, Long>> keyedTaskEvents = filterTaskEvents.keyBy("priority")
                .process(new KeyedProcessFunction<Tuple, TaskEvent, Tuple2<Integer, Long>>() {
                    private MapState<Integer, Long> minMaxSubmitState;//1 = min, 2 = max for key
                    private MapState<Integer, Long> minMaxSchState;//1 = min, 2 = max for key
                    private ValueState<Tuple2<Long, Long>> boundariesState;
                    private MapState<Long, TaskEvent> latestSubmitState ;

                    @Override
                    public void processElement(TaskEvent taskEvent, Context context, Collector<Tuple2<Integer, Long>> collector) throws Exception {
//                        System.out.println("WaterMark: " + context.timerService().currentWatermark());
                        if(context.timerService().currentWatermark() < taskEvent.timestamp) {//valid
                            if (context.timestamp() <= te) {
                                context.timerService().registerEventTimeTimer(te);
                                //waste
                                if (taskEvent.eventType.getValue() == 0) {//if its a submit event
                                    if (minMaxSubmitState.contains(MIN)) {
                                        //compare with current min
                                        if (taskEvent.timestamp < minMaxSubmitState.get(MIN)) {
                                            //update the min
                                            minMaxSubmitState.put(MIN, taskEvent.timestamp);
                                        }
                                    } else {
                                        minMaxSubmitState.put(MIN, taskEvent.timestamp);
                                    }

                                    if (minMaxSubmitState.contains(MAX)) {
                                        //compare with current min
                                        if (taskEvent.timestamp > minMaxSubmitState.get(MAX)) {
                                            //update the min
                                            minMaxSubmitState.put(MAX, taskEvent.timestamp);
                                        }
                                    } else {
                                        minMaxSubmitState.put(MAX, taskEvent.timestamp);
                                    }
                                } else if (taskEvent.eventType.getValue() == 1) {
                                    //submit event
                                    if (minMaxSchState.contains(MIN)) {
                                        //compare with current min
                                        if (taskEvent.timestamp < minMaxSchState.get(MIN)) {
                                            //update the min
                                            minMaxSchState.put(MIN, taskEvent.timestamp);
                                        }
                                    } else {
                                        minMaxSchState.put(MIN, taskEvent.timestamp);
                                    }

                                    if (minMaxSchState.contains(MAX)) {
                                        //compare with current min
                                        if (taskEvent.timestamp > minMaxSchState.get(MAX)) {
                                            //update the min
                                            minMaxSchState.put(MAX, taskEvent.timestamp);
                                        }
                                    } else {
                                        minMaxSchState.put(MAX, taskEvent.timestamp);
                                    }
                                }
                            } else {
//                                System.out.println(te);
                                if (context.timerService().currentWatermark() > te) {
                                    // detection
                                    //do in onTimer
                                    //events for detection period
                                    //compute
                                    if (taskEvent.eventType.getValue() == 0) {
                                        latestSubmitState.put(taskEvent.timestamp, taskEvent);
                                    } else if (taskEvent.eventType.getValue() == 1) {
                                        System.out.println("Min:" + minMaxSchState.get(MIN));
                                        System.out.println("Max: " + minMaxSchState.get(MAX));
                                        for(TaskEvent e : latestSubmitState.values()) {
                                            Long latency = taskEvent.timestamp - e.timestamp;
                                            if(latency < minMaxSchState.get(MIN) || latency > minMaxSchState.get(MAX)) {
                                                collector.collect(new Tuple2<>(taskEvent.priority, latency));
                                            }
                                        }
                                    }
                                }else{
                                    //event belong to detection, but cant use it yet
                                    //store in a mapState
                                    if (taskEvent.eventType.getValue() == 0) {
                                        latestSubmitState.put(taskEvent.timestamp, taskEvent);
                                    }
                                }
                            }
                        }
                    }
                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<Integer, Long>> out) throws Exception {
                        // training is done, time to fit
                        //first compute the boundaries
                            Long a1, a2, a3, a4;
                            a1 = minMaxSchState.get(MAX) - minMaxSubmitState.get(MAX);
                            a2 = minMaxSchState.get(MAX) - minMaxSubmitState.get(MIN);
                            a3 = minMaxSchState.get(MIN) - minMaxSubmitState.get(MAX);
                            a4 = minMaxSchState.get(MIN) - minMaxSubmitState.get(MIN);
                            //update boundaries
                            Long min = Long.MAX_VALUE;
                            Long[] array = {a1, a2, a3, a4};
                            for (int i = 0; i < array.length; i++) // Loop to find the smallest number in array[]
                            {
                                if (min > array[i] && array[i] > 0)
                                    min = array[i];
                            }
                            Long max = Collections.max(Arrays.asList(array));
                            boundariesState.update(new Tuple2<>(min, max));
                    }


                    @Override
                    public void open(Configuration parameters) throws Exception {
                        minMaxSubmitState = getRuntimeContext().getMapState(new MapStateDescriptor<>("curr min max for schedule", Integer.class, Long.class));
                        minMaxSchState = getRuntimeContext().getMapState(new MapStateDescriptor<>("curr min max for submit", Integer.class, Long.class));
                        boundariesState = getRuntimeContext().getState(new ValueStateDescriptor<>("min max bound", TypeInformation.of(new TypeHint<Tuple2<Long, Long>>(){})));
                        latestSubmitState = getRuntimeContext().getMapState(new MapStateDescriptor<>("LatestSubmit",Long.class, TaskEvent.class));

                    }
                });

        printOrTest(keyedTaskEvents);
        env.execute("Ruble Base Anomaly ");
    }



        public void init(String args[]){
        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd = null;
        Options options = new Options();
        Option input1 = new Option("l" , "length", true,"length");
        input1.setRequired(true);

        options.addOption(input1);

        try{
            cmd = parser.parse(options, args);

        }catch (ParseException e){
            System.out.println( "Unexpected exception:" + e.getMessage());
            formatter.printHelp("utility-name", options);
            System.exit(1);
        }
        try {
            te = Integer.parseInt(cmd.getOptionValue("length"));
            System.out.println(te);
        }catch (Exception e){
            System.out.println( "Format error:" + e.getMessage());
            System.out.println( "Format error:" + e.getMessage());
            formatter.printHelp("utility-name", options);
        }
    }
}
