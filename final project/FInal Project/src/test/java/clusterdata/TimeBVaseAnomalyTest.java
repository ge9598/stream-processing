package clusterdata;

import clusterdata.datatypes.EventType;
import clusterdata.datatypes.JobEvent;
import clusterdata.datatypes.TaskEvent;
import clusterdata.exercise1.JobEventCount;
import clusterdata.exercise1.RuleBaseAnomaly;
import clusterdata.exercise1.TimeBaseAnomaly;
import clusterdata.testing.JobEventTestBase;
import clusterdata.testing.TaskEventTestBase;
import com.google.common.collect.Lists;
import org.apache.flink.api.java.tuple.Tuple2;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class TimeBVaseAnomalyTest extends TaskEventTestBase<TaskEvent> {

    static Testable javaExercise = () -> TimeBaseAnomaly.main(new String[]{"-l","5","-s","2","-w","3","-d","0.3"});

    @Test
    public void testRuleBaseAnomalyTest() throws Exception {

        TaskEvent a = testEvent(1L, EventType.SCHEDULE, 5, 60000);// p=1:1
//        TaskEvent b = testEvent(2L, EventType.SCHEDULE, 1, 120000);// p=1:1
        TaskEvent a1 = testEvent(1L, EventType.SCHEDULE, 2, 170000);// p=1:1
//        TaskEvent b1 = testEvent(2L, EventType.SCHEDULE, 1, 240000);// p=1:1
        TaskEvent a2 = testEvent(1L, EventType.SCHEDULE, 8, 290000);// p=1:1
//        TaskEvent b2 = testEvent(2L, EventType.SCHEDULE, 4, 360000);// p=1:1
//        TaskEvent c = testEvent(3L, EventType.SCHEDULE, 3, 420000);// p=1:1
//        TaskEvent c1 = testEvent(3L, EventType.SCHEDULE, 4, 480000);// p=1:1
        TaskEvent a3 = testEvent(1L, EventType.SCHEDULE, 12, 530000);// p=1:1
 ;// p=1:1





        TestTaskEventSource source = new TestTaskEventSource(a,  a1, a2, a3);
        //only for test and no watermarks, which is why omit answers. Able to add it


        assertEquals(Lists.newArrayList(new Tuple2<Long, Long>(1L, 1L),
                new Tuple2<Long, Long>(2L, 1L),
                new Tuple2<Long, Long>(1L, 2L),
                new Tuple2<Long, Long>(3L, 1L),
                new Tuple2<Long, Long>(3L, 2L),
                new Tuple2<Long, Long>(1L, 3L)), results(source));
    }


    private TaskEvent testEvent( Long machineId, EventType type,  long maxCPU, long timestamp) {
        return new TaskEvent(1, 1, timestamp, machineId, type, "Kate",
                2, 1, maxCPU, 0d, 0d, false, "123");
    }
    protected List<?> results(TestTaskEventSource source) throws Exception {
        return runApp(source, new TestSink<>(), javaExercise);
    }

}