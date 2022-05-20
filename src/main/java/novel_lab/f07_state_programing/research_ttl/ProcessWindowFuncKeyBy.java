package novel_lab.f07_state_programing.research_ttl;

import novel_lab.f00_pojo.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
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

public class ProcessWindowFuncKeyBy {
//调查实验key()window()组合的变量的生命周期.
    //实验结果:
    //层次: father_task唯一> sub_task唯一 > key唯一 > 窗口唯一 > 单条数据唯一.
    // 任务类属性: 此处是针对一个算子任务编程, fathertask唯一
    // process函数类属性: 此处是针对1个key, 因此,processFieldCount,应该是key唯一.
    // process函数变量: 此处是针对1个key,1个窗口编程 因此,processCount,应该是key_window唯一.

        public static int classFieldCount = 0;//此处是针对一个算子任务编程, fathertask唯一
    //java的变量, 不具有scala闭包的特性.
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        int mainvalCount = 0;
//        DataStreamSource<String> progSource = env.addSource(new ProgSource(1,"G:\\flink_project_study\\output\\page_action_split\\2022-04-18--15\\.part-0-0.inprogress.3e490f62-fbf6-4a29-8828-e6c7a5b30f2a"));
        DataStreamSource<String> progSource = env.readTextFile("G:\\flink_project_study\\output\\page_action_split\\2022-04-18--15\\.part-0-0.inprogress.3e490f62-fbf6-4a29-8828-e6c7a5b30f2a");
        env.setParallelism(1);
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
//                .filter(data->data.action.equals("PAGE_READ"))
                .keyBy(data->data.action)
                .window(TumblingEventTimeWindows.of(Time.seconds(1)))
                //相同key被分到了同一个子任务中,进行reduce
                .process(new ProcessWindowFunction<Event, Object, String, TimeWindow>() {
                    private int processFieldCount = 0;
                    //尝试对比并行度1和2,发现
                    //此处是针对1个子任务编程, 因此,processFieldCount,应该是sub_task唯一
                    private ValueState<Integer> keyStateCount;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        keyStateCount = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("keyStateCount", Types.INT));
                    }
                    @Override
                    public void process(String s, Context context, Iterable<Event> elements, Collector<Object> out) throws Exception {
                        out.collect("window= " + new Timestamp(context.window().getStart()));
                        //此处是针对1个key,1个窗口编程 因此,processCount,应该是key_window唯一.
                        int processCount = 0;
                        for(Event event: elements){
                            classFieldCount++;
//                            mainvalCount++;
                            processFieldCount++;
                            processCount++;
                            if(keyStateCount.value() == null){//keyState类似process函数内变量, 但他生命周期不因函数结束而结束.
                                keyStateCount.update(1);
                            }else{
                                keyStateCount.update(keyStateCount.value()+1);
                            }
                            out.collect("event= " + event);
                        }
                        out.collect("classFieldCount= " + classFieldCount);
                        out.collect("processFieldCount= " + processFieldCount);
                        out.collect("processCount= " + processCount);
                        out.collect("keyStateCount=" + keyStateCount.value());
                        out.collect("===========================");
                    }
                }).print();
        env.execute();
    }

}
