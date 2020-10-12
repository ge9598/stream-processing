package clusterdata.exercise1;
import clusterdata.datatypes.TaskEvent;;

import clusterdata.utils.TaskEventSchema;
import org.apache.commons.cli.*;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;
import java.util.Properties;

import static clusterdata.utils.AppBase.printOrTest;
import static clusterdata.utils.AppBase.taskSourceOrTest;


public class RuleBaseAnomaly {
    private static final String LOCAL_KAFKA_BROKER = "localhost:9092";
    private static final String LOCAL_ZOOKEEPER_HOST = "localhost:2181";
    private static final String TASKS_GROUP = "taskGroup";
    static int e1;
    static int e2;
    static int e3;
    static int tc;
    static int in;
    public static void main(String[] args) throws Exception {
        RuleBaseAnomaly rba = new RuleBaseAnomaly();
        rba.init(args);
        ParameterTool params = ParameterTool.fromArgs(args);


        // set up streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//       env.getConfig().setLatencyTrackingInterval(500);

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(1000);
        env.enableCheckpointing(in, CheckpointingMode.EXACTLY_ONCE);
        //EO: will be reflex to program once
        //Incremental:
        // set the parallelism
        // TODO: check that your program works correctly with higher parallelism
        env.setParallelism(1);

        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("zookeeper.connect", LOCAL_ZOOKEEPER_HOST);
        kafkaProps.setProperty("bootstrap.servers", LOCAL_KAFKA_BROKER);
        kafkaProps.setProperty("auto.offset.reset", "earliest");
        kafkaProps.setProperty("group.id", TASKS_GROUP);

        FlinkKafkaConsumer011<TaskEvent> consumer = new FlinkKafkaConsumer011<>(
                FilterTaskEventsToKafka.FILTERED_TASKS_TOPIC,
                new TaskEventSchema(),//deserialization schema
                kafkaProps);
        consumer.assignTimestampsAndWatermarks(new MaxTaskCompletionTimeFromKafka.TSExtractor());
        consumer.setStartFromEarliest();
        DataStream<TaskEvent> events = env.addSource(taskSourceOrTest(consumer)).uid("source-id");
        //key stream by id and taskIndex
//        DataStream<TaskEvent> taskEvents = env
//                .addSource(taskSourceOrTest(new TaskEventSource(input, maxEventDelaySecs, servingSpeedFactor)))
//                //compute when its safe to compute, based on the watermark
//                .setParallelism(1);
        DataStream<TaskEvent> filterTaskEvents = events.filter(new FilterFunction<TaskEvent>() {
            @Override
            public boolean filter(TaskEvent taskEvent) throws Exception {
                if(taskEvent.eventType.getValue() == e1 || taskEvent.eventType.getValue() == e2 || taskEvent.eventType.getValue() == e3){
                    return true;
                }
                else {
                    return false;
                }
            }
        }).uid("filter-id");
        DataStream<TaskEvent> keyedTaskEvents = filterTaskEvents.keyBy("jobId", "taskIndex");
        System.out.println("loading Process Function");
        //should use process function
        DataStream<Tuple3<TaskEvent, TaskEvent, TaskEvent> > RuleBaseAnomaly = keyedTaskEvents.process(new ProcessFunction<TaskEvent, Tuple3<TaskEvent, TaskEvent, TaskEvent>>() {
            private MapState<Integer, TaskEvent> e1State;//event type, tast event
            @Override
            public void processElement(TaskEvent taskEvent, Context context, Collector<Tuple3<TaskEvent, TaskEvent, TaskEvent>> collector) throws Exception {
                //ontimer first element timestamp + period
                if(context.timerService().currentWatermark() < taskEvent.timestamp) {//valid //return the current watermark//compare to event.timestamp to watermark
                    if(taskEvent.eventType.getValue() == e1) {
                        if(e1State.get(e1)!= null) {
                            //compare
                            if(e1State.get(e1).timestamp + tc >taskEvent.timestamp){
                                e1State.put(e1, taskEvent);
                            }
                        }else {
                            e1State.put(e1, taskEvent);
                        }
                        context.timerService().registerEventTimeTimer(context.timestamp() + tc);
//                        System.out.println("Initiate e1: " + taskEvent.timestamp);

                    }else if(taskEvent.eventType.getValue() == e2) {
                        if(e1State.get(e2)!= null) {

                            if(e1State.get(e2).timestamp + tc >taskEvent.timestamp){
                                e1State.put(e2, taskEvent);
                            }
                        }else {

                            e1State.put(e2, taskEvent);
                        }


                    }else if(taskEvent.eventType.getValue() == e3) {
                        if(e1State.get(e3)!= null) {
                            if(e1State.get(e3).timestamp + tc >taskEvent.timestamp){
                                e1State.put(e3, taskEvent);
                            }
                        }else {
                            e1State.put(e3, taskEvent);
                        }


                    }
                }
            }

            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple3<TaskEvent, TaskEvent, TaskEvent>> out) throws Exception {
                //Compute
                if(e1State.contains(e1) && e1State.contains(e2) && e1State.contains(e3)) {
//                    List List1 = e1State.get(e1);
//                    List List2 = e1State.get(e2);
//                    List List3 = e1State.get(e3);
//                    for(int i = 0; i < List1.size(); i++){
//                        for(int j = 0; j < List2.size(); j++){
//                            for (int k = 0; k < List3.size(); k++){
//
//                            }
//                        }
//                    }
                    if(e1State.get(e1).timestamp < e1State.get(e2).timestamp && e1State.get(e2).timestamp < e1State.get(e3).timestamp) {
                        System.out.println("Current TimeStamps: e1: " + e1State.get(e1).timestamp + " e2: " + e1State.get(e2).timestamp + " e3: " + e1State.get(e3).timestamp);
                    }
                    if (e1State.get(e1).timestamp < e1State.get(e2).timestamp && e1State.get(e2).timestamp < e1State.get(e3).timestamp
                            && (e1State.get(e3).timestamp - e1State.get(e1).timestamp) < tc) {
                        System.out.println("Output: " + e1State.get(e1).timestamp + " " + e1State.get(e2).timestamp + " " + e1State.get(e3).timestamp);
                        out.collect(new Tuple3<>(e1State.get(e1), e1State.get(e2), e1State.get(e3)));
                        e1State.clear();
                    }
                }

            }

            @Override
            public void open(Configuration parameters) throws Exception {
                e1State = getRuntimeContext().getMapState(new MapStateDescriptor<>("e1State",Integer.class, TaskEvent.class));
            }
        }).uid("Ruler-id");
        printOrTest(RuleBaseAnomaly);
        env.execute("Ruble Base Anomaly ");

    }

    public void init(String args[]){
        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd = null;
        Options options = new Options();
        Option input1 = new Option("e1" , "e1", true,"first Rule(Int)");
        input1.setRequired(true);
        Option input2 = new Option("e2" , "e2", true,"second Rule(Int)");
        input2.setRequired(true);
        Option input3 = new Option("e3" , "e3", true,"third Rule(Int)");
        input3.setRequired(true);
        Option input4 = new Option("tc" , "time-constraint", true,"Time Constraint");
        input4.setRequired(true);
        Option input5 = new Option("in" , "interval", true,"interval");
        options.addOption(input1);
        options.addOption(input2);
        options.addOption(input3);
        options.addOption(input4);
        options.addOption(input5);
        try{
            cmd = parser.parse(options, args);

        }catch (ParseException e){
            System.out.println( "Unexpected exception:" + e.getMessage());
            formatter.printHelp("utility-name", options);
            System.exit(1);
        }
        try {
            e1 = Integer.parseInt(cmd.getOptionValue("e1"));
            e2 = Integer.parseInt(cmd.getOptionValue("e2"));
            e3 = Integer.parseInt(cmd.getOptionValue("e3"));
            tc = Integer.parseInt(cmd.getOptionValue("time-constraint"));
            in = Integer.parseInt(cmd.getOptionValue("in"));
            System.out.println(e1 + " " + e2 + " " + e3 + " " + tc + " " + in);
        }catch (Exception e){
            System.out.println( "Format error:" + e.getMessage());
            formatter.printHelp("utility-name", options);
        }
    }

}
