package novel_lab.f01_source_operator;

import novel_lab.f00_pojo.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

public class FromArray {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        ArrayList<Event> arr = new ArrayList();
        arr.add(new Event("huawei","show",10001L));
        arr.add(new Event("huawei","click",10002L));
        arr.add(new Event("huawei","begin",10003L));
        arr.add(new Event("huawei","read",10004L));
        arr.add(new Event("huawei","close",10005L));

        DataStreamSource<Event> dataStream = env.fromCollection(arr);
        dataStream.print();

//        dataStream.keyBy(data->data.user_id).process(new KeyedProcessFunction<String, Event, Object>() {
//            @Override
//            public void processElement(Event value, Context ctx, Collector<Object> out) throws Exception {
//
//
//            }
//        })
//        dataStream.process(new ProcessFunction<Event, Object>() {
//            @Override
//            public void processElement(Event value, Context ctx, Collector<Object> out) throws Exception {
//
//            }
//        })
        env.execute();
    }
}
