package big.data.hse.hw.hw3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

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
        ArrayList<Double> arrX = new ArrayList<Double>();
        ArrayList<Double> arrY = new ArrayList<Double>();

        int cnt = 0;
        double sumX = 0;
        double sumY = 0;
        for (Text value : values) {
            cnt += 1;
            String[] splitted = value.toString().trim().split("\t");
            double x = Double.parseDouble(splitted[0]);
            arrX.add(x);
            double y = Double.parseDouble(splitted[1]);
            arrY.add(y);

            sumX += x;
            sumY += y;
        }

        double averageX = sumX / cnt;
        String averageStrX = String.format("%10.2f", averageX);
        double averageY = sumY / cnt;
        String averageStrY = String.format("%10.2f", averageY);

        double dX = 0;
        for (Double x: arrX) {
            dX += (x - averageX) * (x - averageX) / (cnt - 1);
        }
        dX = Math.sqrt(dX);
        String dStrX = String.format("%10.2f", dX);

        double dY = 0;
        for (Double y: arrY) {
            dX += (y - averageY) * (y - averageY) / (cnt - 1);
        }
        dY = Math.sqrt(dY);
        String dStrY = String.format("%10.2f", dY);



        context.write(key, new Text(averageStrX + "\t" + averageStrY + "\t" + dStrX + "\t" + dStrY));
    }

}


