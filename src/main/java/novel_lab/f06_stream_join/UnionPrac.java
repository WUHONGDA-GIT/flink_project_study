package novel_lab.f06_stream_join;

import novel_lab.f00_pojo.Event;
import novel_lab.f01_source_operator.ProgSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;


import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.Duration;

public class UnionPrac {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> progSource1 = env.addSource(new ProgSource("G:\\flink_project_study\\input\\watermarktest"));

        SingleOutputStreamOperator<Event> stream1 = progSource1.map(data -> {
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
        }));


        DataStreamSource<String> progSource2 = env.addSource(new ProgSource("G:\\flink_project_study\\input\\watermarktest"));
        SingleOutputStreamOperator<Event> stream2 = progSource2.map(data -> {
            String[] split = data.split(",");
            String user_id = split[0];
            String action = split[1];
            Long timeStamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(split[2]).getTime();
            return new Event(user_id, action, timeStamp);
        }).assignTimestampsAndWatermarks(WatermarkStrategy
                //合流, 以最慢的水位线为合流后的水位线.
                .<Event>forBoundedOutOfOrderness(Duration.ofMinutes(15))
                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event element, long recordTimestamp) {
                        return element.timeStamp;
                    }
                }));
        stream1.print("stream1");
        stream2.print("stream2");

        stream1.union(stream2)
                .process(new ProcessFunction<Event, String>() {
                    @Override
                    public void processElement(Event value, Context ctx, Collector<String> out) throws Exception {
                        out.collect("水位线= " + new Timestamp(ctx.timerService().currentWatermark()));
                    }
                }).print();


//
//        stream1.connect(stream2).process(new CoProcessFunction<Event, Event, Object>() {
//            @Override
//            public void processElement1(Event value, Context ctx, Collector<Object> out) throws Exception {
//            ctx.timerService().currentWatermark();
//            }
//
//            @Override
//            public void processElement2(Event value, Context ctx, Collector<Object> out) throws Exception {
//                ctx.timerService().currentWatermark();
//            }
//        })

        env.execute();
    }
}
