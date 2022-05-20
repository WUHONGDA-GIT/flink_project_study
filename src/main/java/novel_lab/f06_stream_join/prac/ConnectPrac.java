package novel_lab.f06_stream_join.prac;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

public class ConnectPrac {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Tuple3<String, String, Integer>> source1 = env.fromElements(
                new Tuple3<String, String, Integer>("order-1", "app", 1000)
                , new Tuple3<String, String, Integer>("order-2", "app", 2000)
                , new Tuple3<String, String, Integer>("order-3", "app", 3000)
                , new Tuple3<String, String, Integer>("order-4", "app", 4000)
                , new Tuple3<String, String, Integer>("order-5", "app", 5000)
                );

        DataStreamSource<Tuple3<String, String, Integer>> source2 = env.fromElements(
                new Tuple3<String, String, Integer>("order-1", "third-party", 1000)
                , new Tuple3<String, String, Integer>("order-2", "third-party", 12000)
                , new Tuple3<String, String, Integer>("order-3", "third-party", 13000));

        source1.connect(source2)
                .keyBy(data->data.f0,data->data.f0)
                .process(new CoProcessFunction<Tuple3<String, String, Integer>, Tuple3<String, String, Integer>, String>() {
                    //创建状态对象
                    private ValueState<Tuple3<String,String,Integer>> appState;
                    private ValueState<Tuple3<String,String,Integer>> thirdPartState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        //在flink注册分布式状态, 并获取地址到本地.
                        appState = getRuntimeContext().getState(new ValueStateDescriptor<Tuple3<String, String, Integer>>("appState", Types.TUPLE(Types.STRING,Types.STRING,Types.INT)));
                        thirdPartState = getRuntimeContext().getState(new ValueStateDescriptor<Tuple3<String, String, Integer>>("thirdPartState", Types.TUPLE(Types.STRING,Types.STRING,Types.INT)));

                    }

                    @Override
                    public void processElement1(Tuple3<String, String, Integer> value, Context ctx, Collector<String> out) throws Exception {
                        if(thirdPartState.value() != null){
                            out.collect("order=" + value + " third=" + thirdPartState.value() + "匹配成功");
                            thirdPartState.clear();
                            //桶关闭, 其实状态也会删除了, 但此处是为了给onTimer的处理,提供信息, 而不是单纯为了删除状态
                        }else{
                            appState.update(value);
                        }
                        //5秒钟的定时器
                        ctx.timerService().registerEventTimeTimer(value.f2+1);//此处没有给watermark,因此是一个无效的定时器
                        out.collect("souce1 " + ctx.timestamp());//可以看到事件时间时间为空
                        out.collect("souce1 " + ctx.timerService().currentWatermark()); //水位线也是负数,即为空

                    }

                    @Override
                    public void processElement2(Tuple3<String, String, Integer> value, Context ctx, Collector<String> out) throws Exception {
                        if(appState.value() != null){
                            out.collect("order=" + value + " third=" + thirdPartState.value() + "匹配成功");
                            appState.clear();
                            //桶关闭, 其实状态也会删除了, 但此处是为了给onTimer的处理,提供信息, 而不是单纯为了删除状态
                        }else{
                            thirdPartState.update(value);
                        }
                        //实验: 在source没有设置水位线和事件时间
                        ctx.timerService().registerEventTimeTimer(value.f2+1); //此处没有给watermark,因此是一个无效的定时器
                        out.collect("souce2 " + ctx.timestamp()); //可以看到事件时间时间为空
                        out.collect("souce2 " + ctx.timerService().currentWatermark());//水位线也是负数,即为空

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
