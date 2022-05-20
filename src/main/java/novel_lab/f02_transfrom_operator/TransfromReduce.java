package novel_lab.f02_transfrom_operator;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransfromReduce {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> localFileSource = env.readTextFile("input/test_book_info_00001.txt");

        SingleOutputStreamOperator<Tuple5<String,String,String,String,Integer>> mapOper = localFileSource.map(data -> {
            String[] arr = data.split(",");
            String bid = arr[0];
            String title = arr[1];
            String writer = arr[2];
            String gender = arr[4];
            //此处返回值的泛型不能省,不然泛型传递不到keystream
            Tuple5<String,String,String,String,Integer> tuple5 = new Tuple5(bid, title, writer, gender, 1);
            return tuple5;
        }).returns(Types.TUPLE(Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.INT));


        mapOper.keyBy(tuple5 -> tuple5.f3)
                //reduce函数, 汇总所有keyStream, 返回一个DataStream.
                .reduce((base,add)->{
                    base.f4 = base.f4 + add.f4;
                    return base;
                })
                .print();

        env.execute();

    }

}
