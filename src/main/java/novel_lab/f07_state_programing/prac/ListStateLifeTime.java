package novel_lab.f07_state_programing.prac;

import novel_lab.f00_pojo.Event;
import novel_lab.f00_pojo.IdfCount;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.ArrayList;

public class ListStateLifeTime {
    public static void main(String[] args) throws Exception {
        //此方案不需要用到windowAll, 因此性能较高.
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> localFileSource = env.readTextFile("G:\\flink_project_study\\output\\page_action_split\\2022-04-18--15\\topNtest");
        env.setParallelism(1);

        SingleOutputStreamOperator<IdfCount> idfCountWithWindowInfo = localFileSource.map(data -> {
            String[] arr = data.split(",");
            String user_id = arr[0];
            String action = arr[1];
            Long timeStamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(arr[2]).getTime();
            return new Event(user_id, action, timeStamp);
        })
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timeStamp;
                            }
                        }))

                .returns(Types.POJO(Event.class))
                .keyBy(data -> data.user_id)
                .window(TumblingEventTimeWindows.of(Time.hours(1), Time.minutes(30)))
                .aggregate(
                        new AggregateFunction<Event, Tuple2<String, Integer>, Tuple2<String, Integer>>() {
                            @Override
                            public Tuple2<String, Integer> createAccumulator() {
                                return Tuple2.of("", 0);
                            }

                            @Override
                            public Tuple2<String, Integer> add(Event value, Tuple2<String, Integer> accumulator) {
                                return Tuple2.of(value.user_id, accumulator.f1 + 1);
                            }

                            @Override
                            public Tuple2<String, Integer> getResult(Tuple2<String, Integer> accumulator) {
                                return accumulator;
                            }

                            @Override
                            public Tuple2<String, Integer> merge(Tuple2<String, Integer> a, Tuple2<String, Integer> b) {
                                return null;
                            }

//                        }).print();
                        }, new ProcessWindowFunction<Tuple2<String, Integer>, IdfCount, String, TimeWindow>() {
                            @Override
                            public void process(String s, Context context, Iterable<Tuple2<String, Integer>> elements, Collector<IdfCount> out) throws Exception {
                                Tuple2<String, Integer> result = elements.iterator().next();
                                out.collect(new IdfCount(result.f0, result.f1, context.window().getStart(), context.window().getEnd()));
                            }
                        });


        idfCountWithWindowInfo
                .keyBy(data->data.windowEndTime)
                //问题process内定义的状态的生命周期是每个定时器分配一个?
                .process(new KeyedProcessFunction<Long, IdfCount, String>() {
                    private ListState<IdfCount> keyList;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        //为什么需要在此定义状态?  因为状态管理对象要站在程序运行时才能获取, 才能进行注册.
                        keyList = getRuntimeContext().getListState(
                                new ListStateDescriptor<IdfCount>("idfcount_list", Types.POJO(IdfCount.class))
                        );
                    }

                    @Override
                    public void processElement(IdfCount value, Context ctx, Collector<String> out) throws Exception {
                        //每条数据到来的时候, 都调用一次
                        keyList.add(value);
                        ctx.timerService().registerEventTimeTimer(ctx.getCurrentKey() + 1);
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        super.onTimer(timestamp, ctx, out);
                        //实验: 测试得出状态的生命周期是随着ontimer完毕后, 跟着清空的.
                        //对应process函数, 随着每次调用process函数(processElement+ontimer), 都会初始化一个状态.
                        //对应窗口函数, 跟着窗口的关闭而清空, 即每1个窗口都会初始化一个状态.
                        ArrayList<IdfCount> arr = new ArrayList<>();
                        Iterable<IdfCount> iterable = keyList.get();
                        for(IdfCount idfCount: iterable){
                            arr.add(idfCount);
                        }
                        out.collect(" end_in:" + ctx.getCurrentKey() + " arrsize =" + arr.size());
//                        keyList.clear();
                    }


                }).print();

        env.execute();
    }
}
