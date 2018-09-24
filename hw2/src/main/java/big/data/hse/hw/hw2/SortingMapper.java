package big.data.hse.hw.hw2;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by sievmi on 23.09.18
 */

public class SortingMapper extends Mapper<Text, DoubleWritable, Text, DoubleWritable> {
    @Override
    protected void map(Text key, DoubleWritable value, Context context)
            throws IOException, InterruptedException {
        context.write(new Text(String.format(value.toString(), "%10.2f") + key.toString()), value);
    }
}
