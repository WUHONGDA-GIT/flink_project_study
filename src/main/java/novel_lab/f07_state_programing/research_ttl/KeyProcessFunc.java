package novel_lab.f07_state_programing.research_ttl;

import novel_lab.f00_pojo.Event;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;

public class KeyProcessFunc {
    public static int classFieldCount = 0;
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
        })
                .keyBy(data->data.action)
                .process(new KeyedProcessFunction<String, Event, String>() {
                    private int processFieldCount = 0;
                    private ValueState<Integer> keyStateCount;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        keyStateCount = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("keyStateCount", Types.INT));
                    }
                    @Override
                    public void processElement(Event value, Context ctx, Collector<String> out) throws Exception {
                        int processCount = 0;
                        if(classFieldCount==1000){
                            System.exit(0);
                        }
                        classFieldCount++;
//                            mainvalCount++;
                        processFieldCount++;
                        processCount++;
                        if(keyStateCount.value() == null){
                            keyStateCount.update(1);
                        }else{
                            keyStateCount.update(keyStateCount.value()+1);
                        }
                        out.collect("event= " + value);

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
