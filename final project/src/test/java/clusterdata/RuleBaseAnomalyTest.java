package clusterdata;

import clusterdata.datatypes.EventType;
import clusterdata.datatypes.JobEvent;
import clusterdata.datatypes.TaskEvent;
import clusterdata.exercise1.JobEventCount;
import clusterdata.exercise1.RuleBaseAnomaly;
import clusterdata.testing.JobEventTestBase;
import clusterdata.testing.TaskEventTestBase;
import com.google.common.collect.Lists;
import org.apache.flink.api.java.tuple.Tuple2;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class RuleBaseAnomalyTest extends TaskEventTestBase<TaskEvent> {

    static Testable javaExercise = () -> RuleBaseAnomaly.main(new String[]{"-e1","0","-e2","1","-e3","5","-tc","10"});

    @Test
    public void testRuleBaseAnomalyTest() throws Exception {

        TaskEvent a = testEvent(42, 0, 1, EventType.SUBMIT, 0);
        TaskEvent b = testEvent(42, 0, 1, EventType.SCHEDULE, 1);// p=1:1
        TaskEvent c = testEvent(42, 0, 1, EventType.KILL, 2);// p=1:1

        TaskEvent a1 = testEvent(42, 7, 1, EventType.SUBMIT, 4);
        TaskEvent a2 = testEvent(42, 7, 1, EventType.SUBMIT, 5);
        TaskEvent b1 = testEvent(42, 7, 1, EventType.SCHEDULE, 6);// p=3:3
        TaskEvent b2 = testEvent(42, 7, 1, EventType.SCHEDULE, 12);// p=1:10
        TaskEvent b5 = testEvent(42, 7, 1, EventType.SCHEDULE, 7);// p=3:3
        TaskEvent a3 = testEvent(42, 7, 1, EventType.KILL, 13);

        TaskEvent b3 = testEvent(42, 7, 1, EventType.KILL, 14);// p=1:20
        TaskEvent a4 = testEvent(23, 9, 4, EventType.SUBMIT, 66);
        TaskEvent b4 = testEvent(23, 9, 4, EventType.SCHEDULE, 67);
        TaskEvent c4 = testEvent(23, 9, 4, EventType.KILL, 75); //p=4:12

        TestTaskEventSource source = new TestTaskEventSource(a, b, c, a1, a2, b1, b2, b5, a3, b3, a4, b4, c4);
        //only for test and no watermarks, which is why omit answers. Able to add it


        assertEquals(Lists.newArrayList(new Tuple2<Long, Long>(1L, 1L),
                new Tuple2<Long, Long>(2L, 1L),
                new Tuple2<Long, Long>(1L, 2L),
                new Tuple2<Long, Long>(3L, 1L),
                new Tuple2<Long, Long>(3L, 2L),
                new Tuple2<Long, Long>(1L, 3L)), results(source));
    }


    private TaskEvent testEvent(long jobId, int taskIndex, int priority, EventType type, long timestamp) {
        return new TaskEvent(jobId, taskIndex, timestamp, 3, type, "Kate",
                2, priority, 0d, 0d, 0d, false, "123");
    }
    protected List<?> results(TestTaskEventSource source) throws Exception {
        return runApp(source, new TestSink<>(), javaExercise);
    }

}