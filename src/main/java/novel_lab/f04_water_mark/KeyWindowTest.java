package novel_lab.f04_water_mark;

import novel_lab.f00_pojo.Event;
import novel_lab.f01_source_operator.ProgSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.time.Duration;

public class KeyWindowTest {
//调查实验key()window()组合的变量的生命周期.
    public static int classFieldCount = 0;//java的变量, 不具有scala闭包的特性.
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        int mainvalCount = 0;
        DataStreamSource<String> progSource = env.addSource(new ProgSource("G:\\flink_project_study\\input\\watermarktest"));
        progSource.map(data -> {
            String[] split = data.split(",");
            String user_id = split[0];
            String action = split[1];
            Long timeStamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(split[2]).getTime();
            return new Event(user_id, action, timeStamp);
        }).assignTimestampsAndWatermarks(WatermarkStrategy
                .<Event>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event element, long recordTimestamp) {
                        return element.timeStamp;
                    }
                }))
                .keyBy(data->data.action)
                .window(TumblingEventTimeWindows.of(Time.seconds(1)))
                //匿名函数, 属于一个外部类, 不能使用
                .process(new ProcessWindowFunction<Event, Object, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<Event> elements, Collector<Object> out) throws Exception {
                        int processCount = 0;
                        for(Event event: elements){
                            classFieldCount++;
//                            mainvalCount++;
                            processCount++;
                            out.collect("window= " + context.window());
                            out.collect("event= " + event);
                            out.collect("classFieldCount= " + classFieldCount);
                            out.collect("processCount= " + processCount);
                        }
                        out.collect("===========================");
                    }
                }).print();
        env.execute();
    }

}
