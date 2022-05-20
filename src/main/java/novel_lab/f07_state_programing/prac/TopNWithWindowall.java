package novel_lab.f07_state_programing.prac;

import novel_lab.f00_pojo.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;

public class TopNWithWindowall {
    public static void main(String[] args) throws Exception {
        //此方案需要用到windowAll, 因此性能较差.
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> localFileSource = env.readTextFile("G:\\flink_project_study\\output\\page_action_split\\2022-04-18--15\\.part-0-0.inprogress.3e490f62-fbf6-4a29-8828-e6c7a5b30f2a");
        env.setParallelism(1);

        localFileSource.map(data -> {
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
//                .print();
                .windowAll(TumblingEventTimeWindows.of(Time.hours(10)))//不做Keyby的时候开窗使用
                //数据层 ETL,求和汇总数据.
                //窗口层 对数据集合进行排序, 并输出.
                .aggregate(new AggregateFunction<Event, HashMap<String, Long>, ArrayList<Tuple2<String, Long>>>() {
                    @Override
                    public HashMap<String, Long> createAccumulator() {
                        return new HashMap<String, Long>();
                    }

                    @Override
                    public HashMap<String, Long> add(Event value, HashMap<String, Long> accumulator) {
                        if (accumulator.containsKey(value.user_id)) {
                            accumulator.put(value.user_id, accumulator.get(value.user_id) + 1L);
                        }else{
                            accumulator.put(value.user_id, 1L);
                        }
                        return accumulator;
                    }

                    @Override
                    public ArrayList<Tuple2<String, Long>> getResult(HashMap<String, Long> accumulator) {
                        ArrayList<Tuple2<String, Long>> arr = new ArrayList<>();
                        for (String key : accumulator.keySet()) {
                            Long count = accumulator.get(key);
                            arr.add(Tuple2.of(key, count));
                        }
                        arr.sort(new Comparator<Tuple2<String, Long>>() {
                            @Override
                            public int compare(Tuple2<String, Long> o1, Tuple2<String, Long> o2) {
                                //从大到小排序
                                return o2.f1.intValue() - o1.f1.intValue();
                            }
                        });
                        return arr;
                    }

                    @Override
                    public HashMap<String, Long> merge(HashMap<String, Long> a, HashMap<String, Long> b) {
                        return null;
                    }
//                }).print();
                }, new ProcessAllWindowFunction<ArrayList<Tuple2<String, Long>>, String, TimeWindow>() {
                    @Override
                    public void process(Context context, Iterable<ArrayList<Tuple2<String, Long>>> elements, Collector<String> out) throws Exception {
                        ArrayList<Tuple2<String, Long>> arr = elements.iterator().next();
                        String start = new Timestamp(context.window().getStart()).toString();
                        String end = new Timestamp(context.window().getEnd()).toString();
                        int index = 0;
                        if(arr.size()-1 > 10){
                            while((arr.size()-1 >= index) && index < 10  ){
                                int No = index + 1;
                                out.collect(" start_from:" + start + " end_in:" + end + " No="+ No +" user_id="+ arr.get(index).toString());
                                index++;
                            }
                        }
//                        if(arr.size()>0){
//                            out.collect(arr.get(0).toString() + " start_from:" + start + " end_in:" + end);
//                        }
                    }
                }).print();
        env.execute();
    }
}
