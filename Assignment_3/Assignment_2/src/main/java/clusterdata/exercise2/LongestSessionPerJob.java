package clusterdata.exercise2;

import clusterdata.datatypes.JobEvent;
import clusterdata.datatypes.TaskEvent;
import clusterdata.sources.JobEventSource;
import clusterdata.sources.TaskEventSource;
import clusterdata.utils.AppBase;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import java.util.HashMap;
import java.util.Set;

/**
 * Write a program that identifies job stages by their sessions.
 * We assume that a job will execute its tasks in stages and that a stage has finished after an inactivity period of 10 minutes.
 * The program must output the longest session per job, after the job has finished.
 */
public class LongestSessionPerJob extends AppBase {

    public static void main(String[] args) throws Exception {

        ParameterTool params = ParameterTool.fromArgs(args);
        String taskInput = params.get("task_input", pathToTaskEventData);
        String jobInput = params.get("job_input", pathToJobEventData);

        final int servingSpeedFactor = 600; // events of 10 minutes are served in 1 second

        // set up streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setLatencyTrackingInterval(500);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);


        // start the data generator
        DataStream<TaskEvent> taskEvents = env
                .addSource(taskSourceOrTest(new TaskEventSource(taskInput, servingSpeedFactor)))
                .setParallelism(1);
        //filter the taskstream only sumbit and jobstream to only finish, did some pre processing to let both of them output
        // a tuple 3 of id, timestamp, and tasknum/eventtype
        DataStream<JobEvent> jobEvents = env.addSource(jobSourceOrTest(new JobEventSource(jobInput, servingSpeedFactor)));
        DataStream<TaskEvent> filterTask = taskEvents.filter(new FilterFunction<TaskEvent>() {
            @Override
            public boolean filter(TaskEvent taskEvent) throws Exception {
                if(taskEvent.eventType.getValue() == 0){
                    return true;
                }else {
                    return false;
                }
            }
        });
        DataStream<Tuple3<Long, Integer, Long>> sesstionWithNumTask = filterTask.keyBy("jobId")
                .window(EventTimeSessionWindows.withGap(Time.minutes(10)))
                .apply(new WindowFunction<TaskEvent, Tuple3<Long, Integer, Long>, Tuple, TimeWindow>() {

                    @Override
                    public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<TaskEvent> iterable, Collector<Tuple3<Long, Integer, Long>> collector) throws Exception {
                        //calculate the num of tasks within each session
                        int taskNum = 0;
                        for(TaskEvent in : iterable){
                            taskNum ++;
                        }
                        collector.collect(new Tuple3<>(tuple.getField(0), taskNum, timeWindow.getEnd()));
                    }
                });
        DataStream<Tuple3<Long, Integer, Long>> filterJob = jobEvents.filter(new FilterFunction<JobEvent>() {
            @Override
            public boolean filter(JobEvent jobEvent) throws Exception {
                if(jobEvent.eventType.getValue() == 4){
                    return true;
                }
                else{
                    return false;
                }
            }
        }).flatMap(new FlatMapFunction<JobEvent, Tuple3<Long, Integer, Long>>() {
            @Override
            public void flatMap(JobEvent jobEvent, Collector<Tuple3<Long, Integer, Long>> collector) throws Exception {
                collector.collect(new Tuple3<>(jobEvent.jobId, jobEvent.eventType.getValue(), jobEvent.timestamp));
            }
        });
        //connecting the 2 datastream and use on Timer the final output
        DataStream<Tuple2<Long, Integer>> maxSessionPerJob = sesstionWithNumTask
                .connect(filterJob)
                .keyBy(0, 0)
                .process(new CoProcessFunction<Tuple3<Long, Integer, Long>, Tuple3<Long, Integer, Long>, Tuple2<Long, Integer>>() {

                    private ValueState<Tuple3<Long, Integer, Long>> valueState;//priority, elapse
                    //hashmap to update the longest session in each job
                    HashMap<Long, Integer> maxTaskLength = new HashMap<>();
                    @Override
                    public void processElement1(Tuple3<Long, Integer, Long> task, Context context, Collector<Tuple2<Long, Integer>> collector) throws Exception {
                        Tuple3<Long, Integer, Long> curProcess = valueState.value();
                        if(curProcess == null) {
                            valueState.update(task);
                        }else{
                            if(curProcess.f1 >= task.f1) {
                                valueState.update(curProcess);
                            }else{
                                valueState.update(task);
                            }
                        }
//                        if(maxTaskLength.containsKey(task.f0) == false){
//                            maxTaskLength.put(task.f0, task.f1);
//                        }else {
//                            maxTaskLength.put(task.f0, Math.max(task.f1, maxTaskLength.get(task.f0)));
//                        }
                    }

                    @Override
                    public void processElement2(Tuple3<Long, Integer, Long> longIntegerLongTuple3, Context context, Collector<Tuple2<Long, Integer>> collector) throws Exception {
                        context.timerService().registerEventTimeTimer(context.timestamp());//trigger the on timer when the finish job came
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<Long, Integer>> out) throws Exception {

//                        for (Long key : maxTaskLength.keySet()) {
//                            System.out.println(key);
//                            System.out.println();
//                            out.collect(new Tuple2<>(key, maxTaskLength.get(key)));//output the result in the hashmap and clear for the next one.
//                        }
//                        maxTaskLength.clear();

                        Tuple3<Long, Integer, Long> curProcess = valueState.value();
                        Long f0 = curProcess.f0;
                        Integer f1 = curProcess.f1;
                        out.collect(new Tuple2<>(f0,f1));
                        valueState.clear();
                    }

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        valueState = getRuntimeContext().getState(new ValueStateDescriptor<>("valueState", TypeInformation.of(new TypeHint<Tuple3<Long,Integer, Long>>(){})));
                    }
                });


        //TODO: implement the program logic here
        //DataStream<Tuple2<Long, Integer>> maxSessionPerJob = ...
        maxSessionPerJob.addSink(new SinkFunction<Tuple2<Long, Integer>>() {
            @Override
            public void invoke(Tuple2<Long, Integer> value, Context context) throws Exception {

            }
        });

        env.execute("Maximum stage size per job");
    }
}
