package xuwei.tech.custormSource;


import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

public class StreamingDemoWithMyRichPralalleSource {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Long> streamSource = env.addSource(new MyRichParalleSource()).setParallelism(2);


        SingleOutputStreamOperator<Long> operator = streamSource.map(new MapFunction<Long, Long>() {
            @Override
            public Long map(Long value) throws Exception {
                System.out.println("接收到的数据" + value);
                return value;
            }
        });

        SingleOutputStreamOperator<Long> sum = operator.timeWindowAll(Time.seconds(2)).sum(0);

        sum.print().setParallelism(1);

        String name = StreamingDemoWithMyRichPralalleSource.class.getName();
        env.execute(name);
    }
}
