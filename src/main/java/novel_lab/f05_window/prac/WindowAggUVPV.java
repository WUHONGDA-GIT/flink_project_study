package novel_lab.f05_window.prac;

import novel_lab.f00_pojo.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;

import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.HashSet;

public class WindowAggUVPV {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> localFileSource = env.readTextFile("input/page_action_00001");
        env.setParallelism(1);

        localFileSource.map(data->{
            String[] split = data.split(",");
            String user_id = split[3];
            String action = split[20];
            Long timeStamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(split[5]).getTime();
            return new Event(user_id,action,timeStamp);
        })
                //插入水位
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        //数据流顺序类型,数据延迟时间
                        .<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        //指定水位基于的时间
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>(){
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timeStamp;
                            }
                        })

                )
                .keyBy(event->true)
//                .keyBy(event->event.action)
                .window(TumblingEventTimeWindows.of(Time.hours(24*30)))
                .aggregate(new AggregateFunction<Event, Tuple2<Long, HashSet>, String>() {
                    @Override
                    public Tuple2<Long, HashSet> createAccumulator() {
                        return new Tuple2<Long, HashSet>(0L, new HashSet<String>());
                    }

                    @Override
                    public Tuple2<Long, HashSet> add(Event value, Tuple2<Long, HashSet> accumulator) {
                        accumulator.f1.add(value.user_id);
                        return Tuple2.of(accumulator.f0 + 1L, accumulator.f1);
                    }

                    @Override
                    public String getResult(Tuple2<Long, HashSet> accumulator) {
                        return "pv=" + accumulator.f0 + " " + "uv=" + accumulator.f1.size();
                    }

                    @Override
                    public Tuple2<Long, HashSet> merge(Tuple2<Long, HashSet> a, Tuple2<Long, HashSet> b) {
                        return null;
                    }
                }, new ProcessWindowFunction<String, Object, Boolean, TimeWindow>() {
                    @Override
                    public void process(Boolean aBoolean, Context context, Iterable<String> elements, Collector<Object> out) throws Exception {
                     Timestamp start =  new Timestamp(context.window().getStart());
                     Timestamp  end  =  new Timestamp(context.window().getEnd());
                     //在agg和process 结合使用场景中, 输入数据迭代器iterable, 里面的值为1个agg输出的最终结果
                     String uvpv = elements.iterator().next();
                     out.collect(uvpv + " " + start + " " + end);
                    }
                })
                .print();

        env.execute();
    }
}
