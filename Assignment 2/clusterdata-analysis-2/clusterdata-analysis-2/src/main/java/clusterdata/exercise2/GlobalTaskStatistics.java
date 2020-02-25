package clusterdata.exercise2;

import akka.japi.tuple.Tuple4;
import clusterdata.datatypes.TaskEvent;
import clusterdata.sources.TaskEventSource;
import clusterdata.utils.AppBase;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * Count successful, failed, and killed tasks per minute.
 */
public class GlobalTaskStatistics extends AppBase {

    public static void main(String[] args) throws Exception {

        ParameterTool params = ParameterTool.fromArgs(args);
        String input = params.get("input", pathToTaskEventData);

        final int servingSpeedFactor = 600; // events of 10 minutes are served in 1 second

        // set up streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // start the data generator
        DataStream<TaskEvent> taskEvents = env
                .addSource(taskSourceOrTest(new TaskEventSource(input, servingSpeedFactor)))
                .setParallelism(1);

        //TODO: implement the window logic here
        // DataStream<Tuple4<Long, Integer, Integer, Integer>> statistics =
        // printOrTest(statistics);
        DataStream<TaskEvent> filter = taskEvents.filter(taskEvent ->
                taskEvent.eventType.getValue() == 4 || taskEvent.eventType.getValue() == 3 || taskEvent.eventType.getValue() == 5);
        DataStream<TaskEvent> withTimestampsAndWatermarks = filter.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<TaskEvent>() {
            @Override
            public long extractAscendingTimestamp(TaskEvent taskEvent) {
                return taskEvent.timestamp;
            }
        });
        DataStream<Tuple4<Long, Integer, Integer, Integer>> statistics = withTimestampsAndWatermarks
                .windowAll(TumblingEventTimeWindows.of(Time.minutes(1)))
                .process(new ProcessAllWindowFunction<TaskEvent, Tuple4<Long, Integer, Integer, Integer>, TimeWindow>() {
                    @Override
                    public void process(Context context, Iterable<TaskEvent> iterable, Collector<Tuple4<Long, Integer, Integer, Integer>> collector) throws Exception {
                        int failCount = 0, succCount = 0, killCount = 0;
                        for(TaskEvent in: iterable)   {
                            if(in.eventType.getValue() == 3){
                                failCount++;
                            }
                            else if(in.eventType.getValue() == 4){
                                succCount++;
                            }
                            else if(in.eventType.getValue() == 5){
                                killCount++;
                            }
                            collector.collect(new Tuple4<>(context.window().getStart(), succCount, failCount, killCount));
                        }
                    }
                });
        printOrTest(statistics);


        env.execute("Global task statistics");
    }
}
