package clusterdata;

import clusterdata.datatypes.EventType;
import clusterdata.datatypes.JobEvent;
import clusterdata.exercise1.JobEventCount;
import clusterdata.testing.JobEventTestBase;
import com.google.common.collect.Lists;
import org.apache.flink.api.java.tuple.Tuple2;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class JobEventCountTest extends JobEventTestBase<JobEvent> {

	static Testable javaExercise = () -> JobEventCount.main(new String[]{});

	@Test
	public void testEventCount() throws Exception {

		JobEvent a1 = testEvent(1, 0);
		JobEvent b1 = testEvent(2, 1);
		JobEvent a2 = testEvent(1, 2);
		JobEvent c1 = testEvent(3, 3);
		JobEvent c2 = testEvent(3, 4);
		JobEvent a3 = testEvent(1, 5);

		TestJobEventSource source = new TestJobEventSource(a1, b1, a2, c1, c2, a3);

		assertEquals(Lists.newArrayList(new Tuple2<Long, Long>(1L, 1L),
				new Tuple2<Long, Long>(2L, 1L),
				new Tuple2<Long, Long>(1L, 2L),
				new Tuple2<Long, Long>(3L, 1L),
				new Tuple2<Long, Long>(3L, 2L),
				new Tuple2<Long, Long>(1L, 3L)), results(source));
	}

	private JobEvent testEvent(long id, long timestamp) {
		return new JobEvent(id, timestamp, EventType.SUBMIT, "Kate",
				"job42", "logicalJob42", 0, "");
	}

	protected List<?> results(TestJobEventSource source) throws Exception {
		return runApp(source, new TestSink<>(), javaExercise);
	}

}