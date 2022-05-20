package novel_lab.f02_transfrom_operator;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransfromMaxBy {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> localFileSource = env.readTextFile("input/test_book_info_00001.txt");
        //maxby 更接近SQL的max函数
        localFileSource.map(data -> {
            String[] arr = data.split(",");
            String bid = arr[0];
            String title = arr[1];
            String writer = arr[2];
            String gender  = arr[4];
            Tuple5 tuple5 = new Tuple5(bid,title,writer,gender,1);
            return tuple5;
        })
                .returns(Types.TUPLE(Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.INT))
                .keyBy(tuple5 -> tuple5.f4)
                //输入元组的索引,提取数据作为加法的参数
                .maxBy(0)
                .print("maxby: ");

        //max函数与SQL的max函数不一样
        localFileSource.map(data -> {
            String[] arr = data.split(",");
            String bid = arr[0];
            String title = arr[1];
            String writer = arr[2];
            String gender  = arr[4];
            Tuple5 tuple5 = new Tuple5(bid,title,writer,gender,1);
            return tuple5;
        })
                .returns(Types.TUPLE(Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.INT))
                .keyBy(tuple5 -> tuple5.f4)
                //输入元组的索引,提取数据作为加法的参数
                .max(0)
                .print("max: ");
        env.execute();

    }
}
