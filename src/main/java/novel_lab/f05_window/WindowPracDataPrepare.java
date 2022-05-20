package novel_lab.f05_window;

import novel_lab.f00_pojo.Event;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.text.SimpleDateFormat;

public class WindowPracDataPrepare {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamingFileSink<String> stringStreamingFileSink =
                StreamingFileSink.<String>forRowFormat(new Path("./output/page_action_split"), new SimpleStringEncoder<>("UTF-8"))
                        .withRollingPolicy(
                                DefaultRollingPolicy.builder().build())
                        .build();

        DataStreamSource<String> localFileSource = env.readTextFile("input/page_action_00001");
        env.setParallelism(1);
        localFileSource.map(data->{
            String[] split = data.split(",");
            String user_id = split[3];
            String action = split[20];
            Long timeStamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(split[5]).getTime();
            return new Event(user_id,action,timeStamp).toString();
        }).addSink(stringStreamingFileSink);

        env.execute();

    }
}
