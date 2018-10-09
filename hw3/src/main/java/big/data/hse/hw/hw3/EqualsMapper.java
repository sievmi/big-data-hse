package big.data.hse.hw.hw3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by sievmi on 09.10.18
 */
public class EqualsMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable offset, Text line, Context context)
            throws IOException, InterruptedException {

        try {
            String[] splitted = line.toString().split("\t", 2);

            context.write(new Text(splitted[0]), new Text(splitted[1]));
        } catch (Exception e) {
            context.write(new Text("fail"), new Text(":("));
        }
    }
}
