package novel_lab.f07_state_programing;

import novel_lab.f00_pojo.Event;
import novel_lab.f01_source_operator.ProgSource;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

public class OperatorStateBufferSink {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> progSource = env.addSource(new ProgSource("G:\\flink_project_study\\input\\watermarktest"));
        env.enableCheckpointing(5*1000);
        SingleOutputStreamOperator<Event> map = progSource.map(data -> {
            String[] split = data.split(",");
            String user_id = split[0];
            String action = split[1];
            Long timeStamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(split[2]).getTime();
            return new Event(user_id, action, timeStamp);
        });


        map.addSink(new BufferingSink(2));
        env.execute();
    }

    public static class BufferingSink implements SinkFunction<Event>, CheckpointedFunction {
        //定义每批元素量
        private final int threshold;
        private List<Event> list;
        private ListState<Event> listState;

        public BufferingSink(int threshold) {
            this.threshold = threshold;
            this.list = new ArrayList<Event>();
        }

        @Override
        public void invoke(Event value, Context context) throws Exception {
            //每条记录调用一次
            if(list.size()<threshold){
                list.add(value);
            }else{
                System.out.println("==批量输出==");
                for(Event e: list){
                    System.out.println(e);
                }
                list.clear();
            }

        }
        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            //每次快照调用一次
            listState.clear();
            for(Event e : list){
                listState.add(e);
            }
            System.out.println("完成一次checkpoint");

        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            //每次初始化(包括故障恢复)调用一次
            System.out.println("成功进行一次初始化");
            list.clear();
            listState = context.getOperatorStateStore().getListState(new ListStateDescriptor<Event>("buffer", Event.class));
            if(context.isRestored()){
                for(Event e : listState.get()){
                    list.add(e);
                }
            }
        }
    }
}
