package novel_lab.f03_sink_operator;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.util.concurrent.TimeUnit;

public class SinkToFile {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> localFileSource = env.readTextFile("input/test_book_info_00001.txt");
        SingleOutputStreamOperator<Tuple4> mapSource = localFileSource.map(data -> {
            String[] arr = data.split(",");
            String bid = arr[0];
            String title = arr[1];
            String writer = arr[2];
            String type = arr[3];
            String gender = arr[4];
            Tuple4 tuple4 = new Tuple4(bid, title, writer, gender);
            return tuple4;
        })
                .returns(Types.TUPLE(Types.STRING, Types.STRING, Types.STRING, Types.STRING));


        StreamingFileSink<String> stringStreamingFileSink =
                StreamingFileSink.<String>forRowFormat(new Path("./output/"), new SimpleStringEncoder<>("UTF-8"))
                        .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withInactivityInterval(TimeUnit.MINUTES.toMillis(5))
                                .withMaxPartSize(1024 * 1024 * 1024)
                                .withRolloverInterval(TimeUnit.MINUTES.toMillis(1)).build()
                )
                .build();
        mapSource
                .map(data->data.toString())
                .addSink(stringStreamingFileSink);

        env.execute();
    }
}
