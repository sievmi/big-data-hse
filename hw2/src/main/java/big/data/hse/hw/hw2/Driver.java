package big.data.hse.hw.hw2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

/**
 * Created by sievmi on 23.09.18
 */

public class Driver extends Configured implements Tool {

    private static Logger logger = Logger.getLogger(Driver.class);

    public static void main(String[] args) throws Exception {
        try {
            int res = ToolRunner.run(new Configuration(), (Tool) new Driver(), args);
            System.exit(res);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(255);
        }
    }

    public int run(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job1 = Job.getInstance(conf, "AverageWords");

        job1.setJarByClass(Driver.class);
        if (args.length < 3) {
            logger.warn("to run this jar are necessary at 3 parameters");
            return 1;
        }

        // first job
        job1.setMapperClass(WordcountMapper.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(IntWritable.class);
        job1.setReducerClass(WordcountReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(DoubleWritable.class);
        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);
        Path filePath = new Path(args[0]);
        FileInputFormat.setInputPaths(job1, filePath);
        Path outputPath = new Path(args[1]);
        FileOutputFormat.setOutputPath(job1, outputPath);
        job1.waitForCompletion(true);

        //second job
        Path finalOutputPath = new Path(args[2]);
        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2);
        job2.setSortComparatorClass(DecreasingTextComparator.class);
        job2.setJarByClass(Driver.class);
        job2.setMapperClass(SortingMapper.class);
        job2.setReducerClass(SortingReducer.class);
        job2.setNumReduceTasks(5);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.setInputPaths(job2, outputPath);
        FileOutputFormat.setOutputPath(job2, finalOutputPath);
        job2.waitForCompletion(true);

        return 0;
    }

    static class DecreasingTextComparator extends Text.Comparator {
        public DecreasingTextComparator() {
        }

        public int compare(WritableComparable a, WritableComparable b) {
            return -super.compare(a, b);
        }

        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return -super.compare(b1, s1, l1, b2, s2, l2);
        }
    }
}
