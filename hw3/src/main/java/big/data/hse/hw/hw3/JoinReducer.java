package big.data.hse.hw.hw3;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Created by sievmi on 09.10.18
 */
public class JoinReducer extends Reducer<Text, Text, IntWritable, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        // match_id player_name placement
        // match_id player_name x y

        int placement = -1;
        String x = "";
        String y = "";
        for (Text value: values) {
            String[] splitted = value.toString().trim().split("\t");
            if (splitted.length == 3) {
                placement = Integer.parseInt(splitted[2]);
            } else if (splitted.length == 4) {
                 x = splitted[2];
                 y = splitted[3];
            }
        }

        if (placement != -1 && !x.equals("") && !y.equals("")) {
            context.write(new IntWritable(placement), new Text(x + "\t" + y));
        }
    }

}

