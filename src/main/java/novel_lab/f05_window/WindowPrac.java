package novel_lab.f05_window;

import novel_lab.f00_pojo.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.ArrayList;

public class WindowPrac {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.getConfig().setAutoWatermarkInterval(100); //周期性产生watermark


        ArrayList<Event> arr = new ArrayList();
        arr.add(new Event("huawei","show",10001L));
        arr.add(new Event("huawei","click",10002L));
        arr.add(new Event("huawei","begin",10003L));
        arr.add(new Event("huawei","read",10004L));
        arr.add(new Event("huawei","close",10005L));

        DataStreamSource<Event> dataStream = env.fromCollection(arr);
        dataStream.assignTimestampsAndWatermarks(WatermarkStrategy
                .<Event>forBoundedOutOfOrderness(Duration.ofMillis(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event element, long recordTimestamp) {
                        return element.timeStamp;
                    }
                }))
                .keyBy(data->data.user_id)
                .window(TumblingEventTimeWindows.of(Time.seconds(4)))
                .reduce(new ReduceFunction<Event>(){
                    @Override
                    public Event reduce(Event value1, Event value2) throws Exception {
                        value1.action = value1.action + " " + value2.action;
                        return value1;
                    }
                })
                .print();



        env.execute();
    }
}
