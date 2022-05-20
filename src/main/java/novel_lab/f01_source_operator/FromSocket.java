package novel_lab.f01_source_operator;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FromSocket {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> socketSource = env.socketTextStream("localhost", 9999);

        socketSource.map(data -> {
            String[] arr = data.split(",");
            String bid = arr[0];
            String title = arr[1];
            String writer = arr[2];
            String type  = arr[3];
            String gender  = arr[4];
            Tuple4 tuple4 = new Tuple4(bid,title,writer,gender);
            return tuple4;
        })
                .returns(Types.TUPLE(Types.STRING, Types.STRING, Types.STRING, Types.STRING))
                .print();
        env.execute();
    }
}
