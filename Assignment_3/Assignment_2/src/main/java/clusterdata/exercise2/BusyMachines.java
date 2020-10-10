package clusterdata.exercise2;

import clusterdata.datatypes.TaskEvent;
import clusterdata.sources.TaskEventSource;
import clusterdata.utils.AppBase;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Collection;

/**
 * Write a program that identifies every 5 minutes the busiest machines in the cluster during the last 15 minutes.
 * For every window period, the program must then output the number of busy machines during that last period.
 *
 * A machine is considered busy if more than a threshold number of tasks have been scheduled on it
 * during the specified window period (size).
 */
public class BusyMachines extends AppBase {

    public static void main(String[] args) throws Exception {

        ParameterTool params = ParameterTool.fromArgs(args);
        String input = params.get("input", pathToTaskEventData);
        final int busyThreshold = params.getInt("threshold", 15);

        final int servingSpeedFactor = 600; // events of 10 minute are served in 1 second

        // set up streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setLatencyTrackingInterval(500);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // start the data generator
        DataStream<TaskEvent> taskEvents = env
                .addSource(taskSourceOrTest(new TaskEventSource(input, servingSpeedFactor)))
                .setParallelism(1);

        //TODO: implement the program logic here
        //DataStream<Tuple2<Long, Integer>> busyMachinesPerWindow = ...

        //keyby machineId and window it by sliding window in a period of  5 minutes and length of 15 minutes
        DataStream<Tuple2<Long, Integer>> busyMachinesPerWindow = taskEvents.keyBy("machineId")
                .window(SlidingEventTimeWindows.of(Time.minutes(15), Time.minutes(5)))
                .apply(new RichWindowFunction<TaskEvent, Tuple2<Long, Integer>, Tuple, TimeWindow>() {
                    private ListState<TaskEvent> listState;//priority, elapse
                    @Override
                    public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<TaskEvent> iterable, Collector<Tuple2<Long, Integer>> collector) throws Exception {

                        //calculate the num of tasks in each window
                        int taskNum = 0;
                        for(TaskEvent in: iterable) {
                            listState.add(in);
                        }
                        //after calculating, taskNum is over the threshold, flag it by 1 in the tuple
//                        for(TaskEvent t : listState.get()) {
//                            taskNum++;
//                        }
                        if(((Collection<?>)listState.get()).size() >= busyThreshold) {
                            collector.collect(new Tuple2<>(timeWindow.getEnd(), 1));//1 means you are a busy machine
                        }
                        listState.clear();
                    }

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        listState = getRuntimeContext().getListState(new ListStateDescriptor<TaskEvent>("busy Machine", TaskEvent.class));
                    }

                }).keyBy(0)
                //key by each timestamp each window of 5 minutes and sum it together. In this case, the sum wont keep stacking because is the sum function
                // in windowedstream
                .window(TumblingEventTimeWindows.of(Time.minutes(5)))
                .sum(1);
//         Alternative:
//         .process(new KeyedProcessFunction<Tuple, Tuple2<Long, Integer>, Object>() {
//
//            @Override
//            public void processElement(Tuple2<Long, Integer> longIntegerTuple2, Context context, Collector<Object> collector) throws Exception {
//
//            }
//        })
        printOrTest(busyMachinesPerWindow);
        busyMachinesPerWindow.addSink(new SinkFunction<Tuple2<Long, Integer>>() {
            @Override
            public void invoke(Tuple2<Long, Integer> value, Context context) throws Exception {

            }
        });
        env.execute("Number of busy machines every 5 minutes over the last 15 minutes");
    }
}
