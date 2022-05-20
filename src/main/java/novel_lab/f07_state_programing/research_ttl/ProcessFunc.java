package novel_lab.f07_state_programing.research_ttl;

import novel_lab.f00_pojo.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;

import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.time.Duration;

public class ProcessFunc {
    //调查实验ProcessFunc的变量的生命周期.
    public static int classFieldCount = 0;
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        int mainvalCount = 0;
//        DataStreamSource<String> progSource = env.addSource(new ProgSource(1,"G:\\flink_project_study\\output\\page_action_split\\2022-04-18--15\\.part-0-0.inprogress.3e490f62-fbf6-4a29-8828-e6c7a5b30f2a"));
        DataStreamSource<String> progSource = env.readTextFile("G:\\flink_project_study\\output\\page_action_split\\2022-04-18--15\\.part-0-0.inprogress.3e490f62-fbf6-4a29-8828-e6c7a5b30f2a");
        env.setParallelism(10);
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
                .process(new ProcessFunction<Event, String>() {
                    private int processFieldCount = 0;
                    @Override
                    public void processElement(Event value, Context ctx, Collector<String> out) throws Exception {
                        int processCount = 0;
                        classFieldCount++;
//                      mainvalCount++;
                        processFieldCount++;
                        processCount++;
                        out.collect("event= " + value);

                        out.collect("classFieldCount= " + classFieldCount);
                        out.collect("processFieldCount= " + processFieldCount);
                        out.collect("processCount= " + processCount);
                        out.collect("===========================");
                    }
                }).print();

        env.execute();
    }
}
