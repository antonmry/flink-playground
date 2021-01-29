package galiglobal.flink;

import org.junit.Assert;
import org.junit.Test;

public class StreamingJobTest {

    @Test
    public void testMap() throws Exception {
        IncrementMapFunction statelessMap = new IncrementMapFunction();
        Long out = statelessMap.map(1L);
        Assert.assertEquals(2L, out.longValue());
    }
}
