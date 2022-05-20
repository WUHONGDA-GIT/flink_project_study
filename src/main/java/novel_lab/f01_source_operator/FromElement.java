package novel_lab.f01_source_operator;

import novel_lab.f00_pojo.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FromElement {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.getConfig().setAutoWatermarkInterval(100); //周期性产生watermark


        DataStreamSource<Event> dataStream = env.fromElements(
                new Event("huawei", "show", 10001L),
                new Event("huawei", "click", 10002L),
                new Event("huawei", "begin", 10003L),
                new Event("huawei", "read", 10004L),
                new Event("huawei", "close", 10005L)
        );
        dataStream.print();
        env.execute();
    }
}
