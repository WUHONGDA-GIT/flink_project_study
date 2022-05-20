package novel_lab.f04_water_mark;

import novel_lab.f00_pojo.Event;
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

public class WatermarkPrac2 {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source = env.readTextFile("G:\\flink_project_study\\output\\page_action_split\\2022-04-18--15\\.part-0-0.inprogress.3e490f62-fbf6-4a29-8828-e6c7a5b30f2a");
        env.getConfig().setAutoWatermarkInterval(10000);

//        DataStreamSource<String> source = env.readTextFile("G:\\flink_project_study\\input\\watermarktest");
        env.setParallelism(1);
        SingleOutputStreamOperator<Event> dataStream = source.map(data -> {
            String[] split = data.split(",");
            String user_id = split[0];
            String action = split[1];
            Long timeStamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(split[2]).getTime();
            return new Event(user_id, action, timeStamp);
        }).assignTimestampsAndWatermarks(WatermarkStrategy
                .<Event>forBoundedOutOfOrderness(Duration.ofMillis(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event element, long recordTimestamp) {
                        return element.timeStamp;
                    }
                })
        );


        dataStream.process(new ProcessFunction<Event, String>() {
            @Override
            public void processElement(Event value, Context ctx, Collector<String> out) throws Exception {
//                out.collect(value.toString());
                out.collect(new Timestamp(ctx.timerService().currentWatermark()).toString());

            }
        }).print();
        env.execute();
    }
}
