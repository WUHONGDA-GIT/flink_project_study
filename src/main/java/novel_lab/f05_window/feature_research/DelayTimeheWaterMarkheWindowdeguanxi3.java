package novel_lab.f05_window.feature_research;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class DelayTimeheWaterMarkheWindowdeguanxi3 {
    public static class ProgSource implements SourceFunction<String> {
        int timeMills =0;
        public ProgSource(){
        }
        public ProgSource(int timeMills){
            this.timeMills = timeMills;
        }
        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            long[] arr = {1,2,3,2,2,2,2,4,5,6,7};
            for(Long item :  arr) {
                item = item * 1000;
                ctx.collect(item.toString());
                Thread.sleep(timeMills);

            }
            //验证滚动窗口, 1个数据只进入1个桶的结论
        }
        @Override
        public void cancel() {

        }
    }

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> progSource = env.addSource(new DelayTimeheWaterMarkheWindowdeguanxi.ProgSource(1*1000));
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
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(3)))
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
