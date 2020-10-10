package clusterdata.exercise1;

import clusterdata.datatypes.JobEvent;
import clusterdata.sources.JobEventSource;
import clusterdata.utils.AppBase;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;

/**
 * Measure the time between submitting and scheduling each job in the cluster.
 * For each jobID, record the timestamp of the SUBMIT(0) event and look for a subsequent SCHEDULE(1) event.
 * Once received, output their time difference.
 *
 * Note: If a job is submitted multiple times, then measure the latency since the first submission.
 *
 *  Parameters:
 *  --input path-to-input-file
 */
public class JobSchedulingLatency extends AppBase {

    public static void main(String[] args) throws Exception {

        ParameterTool params = ParameterTool.fromArgs(args);
        final String input = params.get("input", AppBase.pathToJobEventData);

        // events of 1 minute are served in 1 second
        // TODO: you can play around with different speed factors
        final int servingSpeedFactor = 600;

        // set up streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Configure the time characteristic; don't worry about this yet
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // set the parallelism
        // TODO: check that your program works correctly with higher parallelism
//        env.setParallelism(1);
        env.getConfig().setLatencyTrackingInterval(500);

        // start the data generator
        DataStream<JobEvent> events = env
                .addSource(jobSourceOrTest(new JobEventSource(input, servingSpeedFactor)))
                .setParallelism(1)                                                                                                                                         ;

        //TODO: implement the following transformations
        // Filter events and only keep submit and schedule
        // DataStream<JobEvent> filteredEvents = ...
        // The results stream consists of tuple-2 records, where field 0 is the jobId and field 1 is the job duration
        // DataStream<Tuple2<Long, Long>> jobIdWithLatency = ...
        // printOrTest(jobIdWithLatency);

        //filter the events to only want submit and schedule
        DataStream<JobEvent> filteredEvents = events.filter(new FilterFunction<JobEvent>() {
            @Override
            public boolean filter(JobEvent jobEvent) throws Exception {
                if (jobEvent.eventType.getValue() == 0 || jobEvent.eventType.getValue() == 1) {
                    return true;
                } else {
                    return false;
                }
            }
        });

        //key by job id
        DataStream<JobEvent> keyedEvent= filteredEvents.keyBy("jobId");

        //use flatmap and a hashmap to calculate the latency of each job
        DataStream<Tuple2<Long, Long>> jobIdWithLatency = keyedEvent.flatMap(new  RichFlatMapFunction<JobEvent, Tuple2<Long, Long>>() {
            private ValueState<JobEvent> jobEventState;
            HashMap<Long, Tuple2<Long,Integer>> latency = new HashMap<>();
            @Override
            public void flatMap(JobEvent jobEvent, Collector<Tuple2<Long, Long>> collector) throws Exception {
                // if there is no key yet, store it in the hashmap first, contains id as key, and a tuple 2 where has timestamp and eventType
                JobEvent curJob = jobEventState.value();
                if(curJob == null){
                    //store
                    jobEventState.update(jobEvent);
                }
                else{
                    if(jobEvent.eventType.getValue() == 1 && curJob.eventType.getValue() == 0){
                        Long time = jobEvent.timestamp - curJob.timestamp;
                        jobEventState.clear();
                        collector.collect(new Tuple2<Long, Long>(jobEvent.jobId,time));
                    }
                    else if(jobEvent.eventType.getValue() == 0 && curJob.eventType.getValue() == 1){
                        Long time = curJob.timestamp - jobEvent.timestamp;
                        jobEventState.clear();
                        collector.collect(new Tuple2<Long, Long>(jobEvent.jobId,time));
                    }
                }
//                if(!latency.containsKey(jobEvent.jobId)){
//                    latency.put(jobEvent.jobId, new Tuple2<Long, Integer>(jobEvent.timestamp, jobEvent.eventType.getValue()));
//                }else{
//                    //if there a key already, calculate the latency based on the eventype of current job event
//                    if(jobEvent.eventType.getValue() == 1 && latency.get(jobEvent.jobId).f1 == 0) {
//                        Long time = jobEvent.timestamp - latency.remove(jobEvent.jobId).f0;
//                        collector.collect(new Tuple2<Long, Long>(jobEvent.jobId,time));
//
//                    }
//                    else if(jobEvent.eventType.getValue() == 0 && latency.get(jobEvent.jobId).f1 == 1){
//                        Long time = latency.remove(jobEvent.jobId).f0 - jobEvent.timestamp;
//                        collector.collect(new Tuple2<Long, Long>(jobEvent.jobId,time));
//                    }
//                }
            }

            @Override
            public void open(Configuration config) throws Exception {
                jobEventState = getRuntimeContext().getState(new ValueStateDescriptor<>("saved id",JobEvent.class));
            }
        });

        jobIdWithLatency.addSink(new SinkFunction<Tuple2<Long, Long>>() {
            @Override
            public void invoke(Tuple2<Long, Long> value, Context context) throws Exception {

            }
        });
        // execute the dataflow

        env.execute("Job Scheduling Latency Application");
    }

}
