package novel_lab.f05_window.prac;

import novel_lab.f00_pojo.Event;
import novel_lab.f01_source_operator.ProgSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.HashSet;

public class WatermarWindowDelayLateDataLab {
    public static void main(String[] args) throws Exception {
//        验证窗口关闭逻辑:
//        例数据:
//        d28e4a2aa6599598eaca6fb78caea4b,PAGE_READ,2021-09-13 00:30:00.0
//        8f51a531eebc6d84f3b7c1209224849b,PAGE_READ,2021-09-13 00:03:59.0
//        26b6642d86cb469b66c25dad0e819be1,PAGE_READ,2021-09-13 00:06:32.0
//        32996c84698f556d93a03f52a22ba8f9,PAGE_READ,2021-09-13 00:08:21.0
//        ff730d4377b194bdcdf2763f156987fe,PAGE_READ,2021-09-13 00:38:30.0
//        分析窗口(0,10), 水位延迟15min, 窗口延迟5min
//        1. 水位线达到窗口右区间点 30 - 15min = 15min
//        2. 水位线达到了 窗口右区间点+延迟事件. 10 + 5min = 15min
//        结论: 窗口延迟大于5min, 则数据进窗成功.
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> progSource = env.addSource(new ProgSource("G:\\flink_project_study\\input\\watermarktest"));
        OutputTag<Event> late = new OutputTag<Event>("late"){};

        SingleOutputStreamOperator<Object> result = progSource.map(data -> {
            String[] split = data.split(",");
            String user_id = split[0];
            String action = split[1];
            Long timeStamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(split[2]).getTime();
            return new Event(user_id, action, timeStamp);
        })
                //插入水位
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        //数据流顺序类型,数据延迟时间
                        .<Event>forBoundedOutOfOrderness(Duration.ofMinutes(15))
                        //指定水位基于的时间
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timeStamp;
                            }
                        })

                )
                .keyBy(event -> true)
                .window(TumblingEventTimeWindows.of(Time.minutes(10)))
                //窗口延迟时间
                .allowedLateness(Time.minutes(5))
                .sideOutputLateData(late)
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
                        Timestamp start = new Timestamp(context.window().getStart());
                        Timestamp end = new Timestamp(context.window().getEnd());
                        //在agg和process 结合使用场景中, 输入数据迭代器iterable, 里面的值为1个agg输出的最终结果
                        String uvpv = elements.iterator().next();
                        out.collect(uvpv + " " + start + " " + end);
                    }
                });

        result.print("result");
        //输出过期数据
        result.getSideOutput(late).print("late");
        //统计过期数据条数
//        result.getSideOutput(late)
//                .map(data->Tuple2.of(data,1))
//                .returns(Types.TUPLE(Types.POJO(Event.class),Types.INT))
//                .keyBy(data->true)
//                .window(TumblingEventTimeWindows.of(Time.hours(24*1000)))
//                .reduce((base,newtup)->{
//                            return Tuple2.of(base.f0,base.f1+newtup.f1);
//                })
//                .print("late");
        env.execute();
    }
}
