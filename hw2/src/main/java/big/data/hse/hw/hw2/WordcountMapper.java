package big.data.hse.hw.hw2;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Created by sievmi on 23.09.18
 */

public class WordcountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    protected void map(LongWritable offset, Text line, Context context)
            throws IOException, InterruptedException {

        String[] splitted = line.toString().split("\t", 2);
        if (splitted.length == 2) {
            String docId = splitted[0];
            String[] words = splitted[1].toLowerCase().split("[^\\p{L}]");

            IntWritable textLength = new IntWritable(words.length);
        /*for (String word : words) {
            context.write(new Text(word), textLength);
        }*/
            context.write(new Text(docId), textLength);
        }

    }
}
