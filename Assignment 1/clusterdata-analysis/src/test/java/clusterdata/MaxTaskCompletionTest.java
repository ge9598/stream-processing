package clusterdata;

import clusterdata.datatypes.EventType;
import clusterdata.datatypes.TaskEvent;
import clusterdata.exercise1.MaxTaskCompletionTimeFromKafka;
import clusterdata.testing.TaskEventTestBase;
import com.google.common.collect.Lists;
import org.apache.flink.api.java.tuple.Tuple2;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class MaxTaskCompletionTest extends TaskEventTestBase<TaskEvent> {

	/**static Testable javaExercise = () -> MaxTaskCompletionTimeFromKafka.main(new String[]{});

	@Test
	public void testMaxDurationPerPriority() throws Exception {

		TaskEvent a = testEvent(42, 0, 1, EventType.SUBMIT, 0);
		TaskEvent b = testEvent(42, 0, 1, EventType.FINISH, 1);// p=1:1
		TaskEvent a1 = testEvent(42, 0, 1, EventType.SUBMIT, 2);
		TaskEvent a2 = testEvent(42, 7, 3, EventType.SUBMIT, 3);
		TaskEvent b1 = testEvent(42, 7, 3, EventType.FINISH, 6);// p=3:3
		TaskEvent b2 = testEvent(42, 0, 1, EventType.FINISH, 12);// p=1:10
		TaskEvent a3 = testEvent(42, 1, 1, EventType.SUBMIT, 13);
		TaskEvent b3 = testEvent(42, 1, 1, EventType.FINISH, 33);// p=1:20

		TaskEvent a4 = testEvent(23, 9, 4, EventType.SUBMIT, 35);
		TaskEvent b4 = testEvent(23, 9, 4, EventType.SUBMIT, 63);
		TaskEvent c4 = testEvent(23, 9, 4, EventType.FINISH, 75); //p=4:12

		TestTaskEventSource source = new TestTaskEventSource(a, b, a1, a2, b1, b2, a3, b3, a4, b4, c4);
		assertEquals(Lists.newArrayList(new Tuple2<Integer, Long>(1, 1L),
				new Tuple2<Integer, Long>(3, 3L),
				new Tuple2<Integer, Long>(1, 10L),
				new Tuple2<Integer, Long>(1, 20L),
				new Tuple2<Integer, Long>(4, 12L)
				), results(source));
	}

	private TaskEvent testEvent(long jobId, int taskIndex, int priority, EventType type, long timestamp) {
		return new TaskEvent(jobId, taskIndex, timestamp, 3, type, "Kate",
				2, priority, 0d, 0d, 0d, false, "123");
	}

	protected List<?> results(TestTaskEventSource source) throws Exception {
		return runApp(source, new TestSink<>(), javaExercise);
	}**/

}