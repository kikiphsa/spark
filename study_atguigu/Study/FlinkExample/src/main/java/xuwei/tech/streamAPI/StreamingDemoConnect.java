package xuwei.tech.streamAPI;


import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import xuwei.tech.custormSource.MyNoParalleSource;

public class StreamingDemoConnect {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Long> test1 = env.addSource(new MyNoParalleSource()).setParallelism(1);
        DataStreamSource<Long> test2 = env.addSource(new MyNoParalleSource()).setParallelism(1);


        SingleOutputStreamOperator<String> test_str = test2.map(new MapFunction<Long, String>() {
            @Override
            public String map(Long value) throws Exception {
                return "str" + value;
            }
        });

        ConnectedStreams<Long, String> connect = test1.connect(test_str);


        SingleOutputStreamOperator<Object> result = connect.map(new CoMapFunction<Long, String, Object>() {
            @Override
            public Object map1(Long value) throws Exception {
                return value;
            }

            @Override
            public Object map2(String value) throws Exception {
                return value;
            }
        });

        result.print().setParallelism(1);

        String name = StreamingDemoConnect.class.getName();

        env.execute(name);


    }
}
