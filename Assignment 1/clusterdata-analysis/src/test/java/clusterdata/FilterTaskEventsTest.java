package clusterdata;

import clusterdata.datatypes.EventType;
import clusterdata.datatypes.TaskEvent;
import clusterdata.exercise1.FilterTaskEventsToKafka;
import clusterdata.testing.TaskEventTestBase;
import com.google.common.collect.Lists;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class FilterTaskEventsTest extends TaskEventTestBase<TaskEvent> {

	/**static Testable javaExercise = () -> FilterTaskEventsToKafka.main(new String[]{});

	@Test
	public void testFilterEvents() throws Exception {

		TaskEvent a = testEvent(EventType.SUBMIT, 0);
		TaskEvent b = testEvent(EventType.FAIL, 1);
		TaskEvent c = testEvent(EventType.SUBMIT, 2);
		TaskEvent d = testEvent(EventType.FAIL, 3);
		TaskEvent e = testEvent(EventType.FINISH, 4);
		TaskEvent f = testEvent(EventType.FINISH, 5);
		TaskEvent g = testEvent(EventType.UPDATE_PENDING, 6);
		TaskEvent h = testEvent(EventType.SCHEDULE, 7);

		TestTaskEventSource source = new TestTaskEventSource(a, b, c, d, e, f, g, h);
		assertEquals(Lists.newArrayList(a, c, e,f), results(source));
	}

	private TaskEvent testEvent(EventType type, long timestamp) {
		return new TaskEvent(42, 9, timestamp, 3, type, "Kate",
				2, 1, 0d, 0d, 0d, false, "123");
	}

	protected List<?> results(TestTaskEventSource source) throws Exception {
		return runApp(source, new TestSink<>(), javaExercise);
	}**/

}