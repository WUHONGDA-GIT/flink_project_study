package novel_lab.f05_window.feature_research;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

//研究超过水位线的后续数据能不能进入到未关闭的窗口中.
//watermark、allowedLateness 、 sideOutputLateData
//案例可知, 滚动窗口是一个左开右闭的窗口


//                      1 1 1 2  1
public class DelayTimeheWaterMarkheWindowdeguanxi {
    public static class ProgSource implements SourceFunction<String> {
        int timeMills =0;
        public ProgSource(){
        }
        public ProgSource(int timeMills){
            this.timeMills = timeMills;
        }
        @Override
        public void run(SourceContext<String> ctx) throws Exception {
//            long[] arr = {1,2,3,4,5,6,7}; //
            long[] arr = {1,2,3,2,2,2,2,4,5,6,7};
            //水位到3,SlidingEventTimeWindows.of(Time.seconds(3), Time.seconds(2))
            //0~3窗口关闭, 2~5窗口还能取到2的数据吗? 验证得到, 是可以的.
            //1000 2000 TimeWindow{start=0, end=3000}
            //2000 3000 2000 2000 2000 2000 4000 TimeWindow{start=2000, end=5000}
            //4000 5000 6000 TimeWindow{start=4000, end=7000}
            //6000 7000 TimeWindow{start=6000, end=9000}
            for(Long item :  arr) {
                item = item * 1000;
                ctx.collect(item.toString());
                Thread.sleep(timeMills);

            }
        }
        @Override
        public void cancel() {

        }
    }

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> progSource = env.addSource(new ProgSource(1*1000));
        //定义一个测输出流标签
        final OutputTag<Long> late = new OutputTag<Long>("late"){};
        SingleOutputStreamOperator<Long> map = progSource.map(data -> {
            return Long.parseLong(data);
        });
        SingleOutputStreamOperator<String> result = map.assignTimestampsAndWatermarks(WatermarkStrategy
                        .<Long>forMonotonousTimestamps()
//                        .<Long>forBoundedOutOfOrderness(Duration.ofMillis(1000))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Long>() {
                            @Override
                            public long extractTimestamp(Long element, long recordTimestamp) {
                                return element;
                            }
                        }))
                .windowAll(SlidingEventTimeWindows.of(Time.seconds(3), Time.seconds(2)))
                //offset = 0
                //1000 2000 TimeWindow{start=0, end=3000}
                //3000 4000 5000 TimeWindow{start=3000, end=6000}
                //6000 7000 TimeWindow{start=6000, end=9000}

                //offset = 1
                //1000 2000 3000 TimeWindow{start=1000, end=4000}
                //4000 5000 6000 TimeWindow{start=4000, end=7000}
                //7000 TimeWindow{start=7000, end=10000}
                .sideOutputLateData(late)
                .process(new ProcessAllWindowFunction<Long, String, TimeWindow>() {
                    @Override
                    public void process(ProcessAllWindowFunction<Long, String, TimeWindow>.Context context, Iterable<Long> iterable, Collector<String> collector) throws Exception {
                        String str = "";
                        for (Long item : iterable) {
                            str = str + item + " ";
                        }
                        //获取窗口信息
                        collector.collect(str + context.window());
                    }
                });

        result.print();
        result.getSideOutput(late).print("late");
        env.execute();
    }





}

