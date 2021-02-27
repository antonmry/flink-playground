package galiglobal.flink.eventTime;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

public class StreamFilter<IN> extends AbstractUdfStreamOperator<IN, FilterFunction<IN>>
    implements OneInputStreamOperator<IN, IN> {

    private static final long serialVersionUID = 4243621562124742653L;

    public StreamFilter() {
        super((FilterFunction<IN>) in -> true);
    }

    @Override
    public void processElement(StreamRecord<IN> element) throws Exception {
        if (userFunction.filter(element.getValue())) {
            output.collect(element);
        }
    }

    @Override
    public void processWatermark(Watermark mark) {
        System.out.println("Watermark: " + mark.getTimestamp());
        output.emitWatermark(mark);
    }
}
