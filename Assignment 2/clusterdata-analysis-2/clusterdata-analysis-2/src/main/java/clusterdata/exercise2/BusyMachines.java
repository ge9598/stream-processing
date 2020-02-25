package clusterdata.exercise2;

import clusterdata.datatypes.TaskEvent;
import clusterdata.sources.TaskEventSource;
import clusterdata.utils.AppBase;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

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

        // start the data generator
        DataStream<TaskEvent> taskEvents = env
                .addSource(taskSourceOrTest(new TaskEventSource(input, servingSpeedFactor)))
                .setParallelism(1);

        //TODO: implement the program logic here
        //DataStream<Tuple2<Long, Integer>> busyMachinesPerWindow = ...
        //printOrTest(busyMachinesPerWindow);

        env.execute("Number of busy machines every 5 minutes over the last 15 minutes");
    }
}
