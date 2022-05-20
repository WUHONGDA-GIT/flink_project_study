package novel_lab.f06_stream_join;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class ConnectPrac {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(1);
        SingleOutputStreamOperator<Tuple3<String, String, Long>> source1 = env.fromElements(
                new Tuple3<String, String, Long>("order-1", "app", 100000L)
                , new Tuple3<String, String, Long>("order-2", "app", 200000L)
                , new Tuple3<String, String, Long>("order-3", "app", 300000L)
                , new Tuple3<String, String, Long>("order-4", "app", 400000L)
                , new Tuple3<String, String, Long>("order-5", "app", 500000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy
                .<Tuple3<String, String, Long>>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, String, Long>>() {
                    @Override
                    public long extractTimestamp(Tuple3<String, String, Long> element, long recordTimestamp) {
                        return element.f2;
                    }
                }));

        SingleOutputStreamOperator<Tuple3<String, String, Long>> source2 = env.fromElements(
                new Tuple3<String, String, Long>("order-1", "third-party", 100000L)
                , new Tuple3<String, String, Long>("order-10", "third-party", 200000L)
                , new Tuple3<String, String, Long>("order-2", "third-party", 1200000L)
                , new Tuple3<String, String, Long>("order-3", "third-party", 1300000L)).assignTimestampsAndWatermarks(WatermarkStrategy
                .<Tuple3<String, String, Long>>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, String, Long>>() {
                    @Override
                    public long extractTimestamp(Tuple3<String, String, Long> element, long recordTimestamp) {
                        return element.f2;
                    }
                }));

        source1.connect(source2)
                .keyBy(data->data.f0,data->data.f0)
                .process(new CoProcessFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>, String>() {
                    //创建状态对象
                    private ValueState<Tuple3<String,String,Long>> appState;
                    private ValueState<Tuple3<String,String,Long>> thirdPartState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        //在flink注册分布式状态, 并获取地址到本地.
                        appState = getRuntimeContext().getState(new ValueStateDescriptor<Tuple3<String, String, Long>>("appState", Types.TUPLE(Types.STRING,Types.STRING,Types.INT)));
                        thirdPartState = getRuntimeContext().getState(new ValueStateDescriptor<Tuple3<String, String, Long>>("thirdPartState", Types.TUPLE(Types.STRING,Types.STRING,Types.INT)));

                    }

                    @Override
                    public void processElement1(Tuple3<String, String, Long> value, Context ctx, Collector<String> out) throws Exception {
                        if(thirdPartState.value() != null){
                            out.collect("order=" + value + " third=" + thirdPartState.value() + "匹配成功");
                            thirdPartState.clear();
                            //桶关闭, 其实状态也会删除了, 但此处是为了给onTimer的处理,提供信息, 而不是单纯为了删除状态
                        }else{
                            appState.update(value);
                        }
                        //为什么水位线没有增长, 无效了呢?
                        //因为水位线的产生基于处理时间, 而本实验中, 数据量太少了, 200ms内,甚至是1ms内, 已经读完数据, 根本没到水位线生成的定时器触发.
                        ctx.timerService().registerEventTimeTimer(value.f2+500000L);
                        out.collect("souce1 timeStamp " + ctx.timestamp());
                        out.collect("souce1 watermark" + ctx.timerService().currentWatermark());

                    }

                    @Override
                    public void processElement2(Tuple3<String, String, Long> value, Context ctx, Collector<String> out) throws Exception {
                        if(appState.value() != null){
                            out.collect("order=" + value + " third=" + thirdPartState.value() + "匹配成功");
                            appState.clear();
                            //桶关闭, 其实状态也会删除了, 但此处是为了给onTimer的处理,提供信息, 而不是单纯为了删除状态
                        }else{
                            thirdPartState.update(value);
                        }
                        //为什么水位线没有增长, 无效了呢?
                        //因为水位线的产生基于处理时间, 而本实验中, 数据量太少了, 200ms内,甚至是1ms内, 已经读完数据, 根本没到水位线生成的定时器触发.
                        ctx.timerService().registerEventTimeTimer(value.f2+500000L);
                        out.collect("souce2 timeStamp" + ctx.timestamp());
                        out.collect("souce2 watermark" + ctx.timerService().currentWatermark());
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        super.onTimer(timestamp, ctx, out);
                        if(appState.value() == null && thirdPartState.value() == null){

                        }else{
                            out.collect("ontimer" + " order=" + appState.value() + " third=" + thirdPartState.value() + "匹配失败");
                        }

                    }
                }).print();


        env.execute();
    }
}
