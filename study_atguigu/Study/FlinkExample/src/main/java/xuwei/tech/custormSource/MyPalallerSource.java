package xuwei.tech.custormSource;

import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

public class MyPalallerSource implements ParallelSourceFunction<Long> {
    @Override
    public void run(SourceContext<Long> ctx) throws Exception {

    }

    @Override
    public void cancel() {

    }
}
