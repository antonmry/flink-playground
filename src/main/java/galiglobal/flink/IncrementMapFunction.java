package galiglobal.flink;

import org.apache.flink.api.common.functions.MapFunction;

// While it's tempting for something this simple, avoid using anonymous classes or lambdas
// for any business logic you might want to unit test.
public class IncrementMapFunction implements MapFunction<Long, Long> {

    @Override
    public Long map(Long record) throws Exception {
        return record + 1;
    }
}
