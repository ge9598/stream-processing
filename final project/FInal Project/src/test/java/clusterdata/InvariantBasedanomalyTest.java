package clusterdata;

import clusterdata.datatypes.EventType;
import clusterdata.datatypes.TaskEvent;
import clusterdata.exercise1.FilterTaskEventsToKafka;
import clusterdata.exercise1.Invariantbasedanomaly;
import clusterdata.exercise1.RuleBaseAnomaly;
import clusterdata.testing.TaskEventTestBase;
import com.google.common.collect.Lists;
import org.apache.flink.api.java.tuple.Tuple2;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class InvariantBasedanomalyTest extends TaskEventTestBase<TaskEvent> {

    static Testable javaExercise = () -> Invariantbasedanomaly.main(new String[]{"-l","10"});

    @Test
    public void testRuleBaseAnomalyTest() throws Exception {

        TaskEvent a = testEvent(0, 0, 1, EventType.SUBMIT, 3);
        TaskEvent b = testEvent(0, 0, 1, EventType.SUBMIT, 5);// p=1:1
        TaskEvent c = testEvent(1, 0, 1, EventType.SCHEDULE, 7);// p=1:1
        TaskEvent c1 = testEvent(1, 0, 1, EventType.SCHEDULE, 9);// p=1:1


        TaskEvent a1 = testEvent(1, 7, 0, EventType.SUBMIT, 11);
        TaskEvent b1 = testEvent(1, 7, 0, EventType.SUBMIT, 12);// p=3:3
        TaskEvent b2 = testEvent(1, 7, 1, EventType.SCHEDULE, 15);// p=1:10
        TaskEvent b5 = testEvent(1, 7, 1, EventType.SCHEDULE, 19);// p=3:3

        TestTaskEventSource source = new TestTaskEventSource(a, b, c,c1, a1, b1, b2, b5);
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