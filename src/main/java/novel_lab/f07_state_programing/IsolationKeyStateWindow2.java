package novel_lab.f07_state_programing;

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

public class IsolationKeyStateWindow2 {
    // 测试
    // process内的jvm本地变量, 随窗口的结束而结束
    // 因为每个process函数调用一次窗口函数.

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
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
                .process(new ProcessWindowFunction<Event, Object, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<Event> elements, Collector<Object> out) throws Exception {
                        int count = 0;
                        for(Event event: elements){
                            count++;
                            out.collect("window= " + context.window());
                            out.collect("event= " + event);
                            out.collect("key= " + event.action + " count= " + count);
                        }
                        out.collect("===========================");
                }
                }).print();
        env.execute();
    }
}
