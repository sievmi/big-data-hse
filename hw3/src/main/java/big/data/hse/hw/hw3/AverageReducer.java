package big.data.hse.hw.hw3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by sievmi on 09.10.18
 */
public class AverageReducer extends Reducer<IntWritable, Text, IntWritable, Text> {

    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        // match_id player_name placement
        // match_id player_name x y

        int placement = key.get();

        int cnt = 0;
        double sumX = 0;
        double sumY = 0;
        for (Text value : values) {
            cnt += 1;
            String[] splitted = value.toString().trim().split("\t");
            double x = Double.parseDouble(splitted[0]);
            double y = Double.parseDouble(splitted[1]);

            sumX += x;
            sumY += y;
        }
        String averageStrX = String.format("%10.2f", sumX / cnt);
        String averageStrY = String.format("%10.2f", sumY / cnt);


        context.write(key, new Text(averageStrX + "\t" + averageStrY));
    }

}


