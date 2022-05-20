package novel_lab.f06_stream_join;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class SideOutPutPrac {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> localFileSource = env.readTextFile("input/test_book_info_00001.txt");
        OutputTag<Tuple4<String, String, String, String>> book_man = new OutputTag<>("book_man",Types.TUPLE(Types.STRING, Types.STRING, Types.STRING, Types.STRING));


        SingleOutputStreamOperator<Tuple4<String, String, String, String>> mainStream = localFileSource.map(data -> {
            String[] arr = data.split(",");
            String bid = arr[0];
            String title = arr[1];
            String writer = arr[2];
            String type = arr[3];
            String gender = arr[4];
            Tuple4<String, String, String, String> tuple4 = new Tuple4(bid, title, writer, gender);
            return tuple4;
        })
                .returns(Types.TUPLE(Types.STRING, Types.STRING, Types.STRING, Types.STRING))
                .process(new ProcessFunction<Tuple4<String, String, String, String>, Tuple4<String, String, String, String>>() {
                    @Override
                    public void processElement(Tuple4<String, String, String, String> value, Context ctx, Collector<Tuple4<String, String, String, String>> out) throws Exception {
                        // 注意 java 字符串对比, 要使用equals.
                        if (value.f3.equals("男频") ) {
                            ctx.output(book_man,value);
                        } else {
                            out.collect(value);
                        }
                    }
                });


        mainStream.print("女频");
        mainStream.getSideOutput(book_man).print("男频");

        env.execute();
    }
}
