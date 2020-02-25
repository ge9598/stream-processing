package clusterdata.exercise2;

import clusterdata.datatypes.TaskEvent;
import clusterdata.sources.TaskEventSource;
import clusterdata.utils.AppBase;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

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
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // start the data generator
        DataStream<TaskEvent> taskEvents = env
                .addSource(taskSourceOrTest(new TaskEventSource(taskInput, servingSpeedFactor)))
                .setParallelism(1);

        //TODO: implement the program logic here
        //DataStream<Tuple2<Long, Integer>> maxSessionPerJob = ...
        //printOrTest(maxSessionPerJob);

        env.execute("Maximum stage size per job");
    }
}
