package novel_lab.f04_water_mark;

import novel_lab.f00_pojo.Event;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.ArrayList;

public class WatermarkPrac {
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
                .withTimestampAssigner(new SerializableTimestampAssigner<Event>(){
                    @Override
                    public long extractTimestamp(Event element, long recordTimestamp) {
                        return element.timeStamp;
                    }
                })
        );

        dataStream.process(new ProcessFunction<Event, String>() {
            @Override
            public void processElement(Event value, Context ctx, Collector<String> out) throws Exception {
                out.collect(value.toString());
                out.collect(ctx.timerService().currentWatermark()+"");

            }
        }).print();

        env.execute();
    }
}
