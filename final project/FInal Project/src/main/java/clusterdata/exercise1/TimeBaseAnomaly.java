package clusterdata.exercise1;

import clusterdata.datatypes.TaskEvent;
import clusterdata.sources.TaskEventSource;
import clusterdata.utils.AppBase;
import clusterdata.utils.TaskEventSchema;
import org.apache.commons.cli.*;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

import static clusterdata.utils.AppBase.printOrTest;
import static clusterdata.utils.AppBase.taskSourceOrTest;

public class TimeBaseAnomaly {
    static long length, slide;
    static int windows;
    static double diff;
    private static final String LOCAL_KAFKA_BROKER = "localhost:9092";
    public static final String FILTERED_TASKS_TOPIC = "filteredTasks";
    private static final String LOCAL_ZOOKEEPER_HOST = "localhost:2181";
    private static final String TASKS_GROUP = "taskGroup";
    public static void main(String[] args) throws Exception {
        TimeBaseAnomaly tba = new TimeBaseAnomaly();
        tba.init(args);

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

        // set the parallelism
        // TODO: check that your program works correctly with higher parallelism
        env.setParallelism(1);
//        env.getConfig().setLatencyTrackingInterval(500);
        // get source data
//        DataStream<TaskEvent> taskEvents = env
//                .addSource(taskSourceOrTest(new TaskEventSource(input, maxEventDelaySecs, servingSpeedFactor)))
//                //compute when its safe to compute, based on the watermark
//                .setParallelism(1);

        //filter jobs
        // serialization schema
        // run the cleansing pipeline
        FlinkKafkaConsumer011<TaskEvent> consumer = new FlinkKafkaConsumer011<>(
                FilterTaskEventsToKafka.FILTERED_TASKS_TOPIC,
                new TaskEventSchema(),//deserialization schema
                kafkaProps);
        consumer.assignTimestampsAndWatermarks(new MaxTaskCompletionTimeFromKafka.TSExtractor());
        consumer.setStartFromEarliest();

        DataStream<TaskEvent> events = env.addSource(taskSourceOrTest(consumer));
        DataStream<TaskEvent> filterTaskEvents = events.filter(new FilterFunction<TaskEvent>() {
            @Override
            public boolean filter(TaskEvent taskEvent) throws Exception {
                if(taskEvent.eventType.getValue() == 1){
                    return true;
                }
                else {
                    return false;
                }
            }
        });
        DataStream<Tuple3<Long, Long, Double>>WindowedTaskEvent = filterTaskEvents.keyBy("machineId")
                .window(SlidingEventTimeWindows.of(Time.minutes(length), Time.minutes(slide)))
                .process(new ProcessWindowFunction<TaskEvent, Tuple3<Long, Long, Double>, Tuple, TimeWindow>() {
                    @Override
                    public void process(Tuple tuple, Context context, Iterable<TaskEvent> iterable, Collector<Tuple3<Long,Long, Double>> collector) throws Exception {
                        Double average;
                        Double total = 0.0;
                        long timestamp = context.window().getEnd();
                        for(TaskEvent t : iterable) {
                            total += t.maxCPU;

                        }
                        average = total / ((Collection<?>) iterable).size();
                        System.out.println();
                        System.out.println("TimeStamp Start: " + context.window().getStart() + " TimeStamp End: "
                                + timestamp + " num in window: " +  ((Collection<?>) iterable).size() + "Machineid: "
                                + tuple.getField(0) + " average: " + average );

                        collector.collect(new Tuple3<>(tuple.getField(0), timestamp, average));// machineid, timestamp, Average
                    }
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                    }
                });
        DataStream<Tuple3<Long, Double, Double>> TimeBaseAnomaly = WindowedTaskEvent.keyBy(0)
                .process(new KeyedProcessFunction<Tuple, Tuple3<Long, Long, Double>, Tuple3<Long, Double, Double>>() {
                    //mapState, 3 of them, calculate average, check exist,
                    private MapState<Long, Tuple3> mapAverageState;// add and update the average we use to calculate current
                    //timestamp, average

                    @Override
                    public void processElement(Tuple3<Long, Long, Double> longLongDoubleTuple3, Context context, Collector<Tuple3<Long, Double, Double>> collector) throws Exception {
                        //machineid, timestamp, Average
                        //store the values in listState
                        mapAverageState.put(longLongDoubleTuple3.f1, longLongDoubleTuple3);
                        //check the length of this listAverageState
                        //register a timer to calculate diff

                        System.out.println(longLongDoubleTuple3.f1);
                        context.timerService().registerEventTimeTimer(longLongDoubleTuple3.f1);
                    }

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        mapAverageState = getRuntimeContext().getMapState(new MapStateDescriptor<Long, Tuple3>
                                ("map State", BasicTypeInfo.LONG_TYPE_INFO, TupleTypeInfo.getBasicTupleTypeInfo(Long.class, Long.class, Double.class)));
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple3<Long, Double, Double>> out) throws Exception {
                        //calculation
                        int num = 0;
                        Double movavg;
                        Double total = 0.0;
                        Double differ;

                        for(Tuple3<Long, Long, Double> e : mapAverageState.values()) {
                            num ++;
                        }
                        if( num >= windows) {
                            Double currAvg;
                            currAvg = (Double) mapAverageState.get(timestamp).f2;
                            System.out.println(currAvg);
                            for(Tuple3<Long, Long, Double> e : mapAverageState.values()) {
                                if((timestamp - e.f1 <= (slide * (windows - 1)) * 60000) && timestamp - e.f1 >= 0){
                                    total = total + e.f2;
                                }
                            }
                            movavg = total / windows;
                            differ = Math.abs(movavg - currAvg) / ((movavg + currAvg) / 2);
                            if( differ > diff ) {
                                System.out.println("df:" + differ);
                                out.collect(new Tuple3<Long, Double, Double>( (Long) mapAverageState.get(timestamp).f0, currAvg, movavg));
                                System.out.println("Current time: " + timestamp);
                                System.out.println("Total: " + total + " currAvg: " + currAvg  + " movAvg: " + movavg);
                            }
                        }




                        //update listState

                    }
                });
        printOrTest(TimeBaseAnomaly);
        env.execute("Ruble Base Anomaly ");


    }
    public void init(String args[]){
        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd = null;
        Options options = new Options();
        Option input1 = new Option("l" , "length", true,"length");
        input1.setRequired(true);
        Option input2 = new Option("s" , "slide", true,"slide");
        input2.setRequired(true);
        Option input3 = new Option("w" , "windows", true,"windows");
        input3.setRequired(true);
        Option input4 = new Option("d" , "diff", true,"diff");
        input4.setRequired(true);
        options.addOption(input1);
        options.addOption(input2);
        options.addOption(input3);
        options.addOption(input4);
        try{
            cmd = parser.parse(options, args);

        }catch (ParseException e){
            System.out.println( "Unexpected exception:" + e.getMessage());
            formatter.printHelp("utility-name", options);
            System.exit(1);
        }
        try {
            length = Long.parseLong(cmd.getOptionValue("length"));
            slide = Long.parseLong(cmd.getOptionValue("slide"));
            windows = Integer.parseInt(cmd.getOptionValue("windows"));
            diff = Double.parseDouble(cmd.getOptionValue("diff"));
            System.out.println(length + " " + slide + " " + windows + " " + diff);
        }catch (Exception e){
            System.out.println( "Format error:" + e.getMessage());
            formatter.printHelp("utility-name", options);
        }
    }

}
