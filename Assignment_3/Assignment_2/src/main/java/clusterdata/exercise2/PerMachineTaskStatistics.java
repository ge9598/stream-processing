package clusterdata.exercise2;

import akka.japi.tuple.Tuple4;
import akka.japi.tuple.Tuple5;
import clusterdata.datatypes.TaskEvent;
import clusterdata.sources.TaskEventSource;
import clusterdata.utils.AppBase;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;

/**
 * Count successful, failed, and killed tasks per machine per minute.
 */
public class PerMachineTaskStatistics extends AppBase {

    public static void main(String[] args) throws Exception {

        ParameterTool params = ParameterTool.fromArgs(args);
        String input = params.get("input", pathToTaskEventData);

        final int servingSpeedFactor = 50; // events of 10 minutes are served in 1 second

        // set up streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setLatencyTrackingInterval(500);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // start the data generator
        DataStream<TaskEvent> taskEvents = env
                .addSource(taskSourceOrTest(new TaskEventSource(input, servingSpeedFactor)))
                .setParallelism(1);


        //TODO: implement the window logic here
        // DataStream<Tuple5<Long, Long, Integer, Integer, Integer>> statistics = ...
        // printOrTest(statistics);

        DataStream<TaskEvent> filter = taskEvents.filter(taskEvent ->
                taskEvent.eventType.getValue() == 4 || taskEvent.eventType.getValue() == 3 || taskEvent.eventType.getValue() == 5);
        //after filtering, complete a keyby machineId first then use tumbling window of 1 minutes to calcuate the num of each tasks in each machine
        DataStream<Tuple5<Long, Long, Integer, Integer, Integer>> statistics = filter
                .keyBy("machineId")
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                .apply(new WindowFunction<TaskEvent, Tuple5<Long, Long, Integer, Integer, Integer>, Tuple, TimeWindow>() {
                    @Override
                    public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<TaskEvent> iterable, Collector<Tuple5<Long, Long, Integer, Integer, Integer>> collector) throws Exception {
                        //similar function in global task(part 1)
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
                        }
                        Long stringId = (Long) tuple.getField(0);
                        Tuple5 output = new Tuple5<>(timeWindow.getStart(),stringId, succCount, failCount, killCount);
                        collector.collect(output);
                    }
                });

        statistics.addSink(new SinkFunction<Tuple5<Long, Long, Integer, Integer, Integer>>() {
            @Override
            public void invoke(Tuple5<Long, Long, Integer, Integer, Integer> value, Context context) throws Exception {

            }
        });
        env.execute("Per machine task statistics");
    }

}
