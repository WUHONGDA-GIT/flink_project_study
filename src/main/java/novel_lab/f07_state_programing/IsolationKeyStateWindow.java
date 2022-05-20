package novel_lab.f07_state_programing;

import novel_lab.f00_pojo.Event;
import novel_lab.f01_source_operator.ProgSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.time.Duration;

public class IsolationKeyStateWindow {
    // 测试
    // 自定义keyState的生命周期不受窗口生命周期影响.
    // 靠自己控制keyState的TTL
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
                    private ReducingState<Long> count;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        count = getRuntimeContext().getReducingState(new ReducingStateDescriptor<Long>("count", new ReduceFunction<Long>() {
                              @Override
                              public Long reduce(Long value1, Long value2) throws Exception {
                                  return value1 + 1L;
                              }
                          }, Long.class));
                    }

                    @Override
                    public void process(String s, Context context, Iterable<Event> elements, Collector<Object> out) throws Exception {
                        for(Event event: elements){
//                            count.update(count.value()+1);
                            count.add(1L);
                            out.collect("window= " + context.window());
                            out.collect("event= " + event);
                            out.collect("key= " + event.action + " count= " + count.get());
                        }
                        out.collect("===========================");
                }
                }).print();




//                .countWindow(2)
//                .process(new ProcessWindowFunction<Event, Object, String, GlobalWindow>() {
////                    private ValueState<Integer> count;
//                    private ReducingState<Long> count;
//                    @Override
//                    public void open(Configuration parameters) throws Exception {
//                        super.open(parameters);
////                        count = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("count", Integer.class));
//                          count = getRuntimeContext().getReducingState(new ReducingStateDescriptor<Long>("count", new ReduceFunction<Long>() {
//                              @Override
//                              public Long reduce(Long value1, Long value2) throws Exception {
//                                  return value1 + 1L;
//                              }
//                          }, Long.class));
//                    }
//
//                    @Override
//                    public void process(String s, Context context, Iterable<Event> elements, Collector<Object> out) throws Exception {
//                        for(Event event: elements){
////                            count.update(count.value()+1);
//                            count.add(1L);
//                            out.collect("window= " + context.window());
//                            out.collect("event= " + event);
//                            out.collect("key= " + event.action + " count= " + count.get());
//                        }
//                        out.collect("===========================");
//                    }
//                }).print();
        env.execute();
    }
}
