package big.data.hse.hw.hw3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
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
 * Created by sievmi on 09.10.18
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

        if (args.length < 2) {
            logger.warn("to run this jar are necessary at 3 parameters");
            return 1;
        }

        String inputDirPath = args[0];

        // 1 job
        Configuration conf = new Configuration();
        Job job1 = Job.getInstance(conf, "AverageWords");
        job1.setJarByClass(Driver.class);
        job1.setMapperClass(AggregateFileMapper.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setNumReduceTasks(0);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);
        Path aggregateFilePath = new Path(inputDirPath + "/aggregate.csv");
        FileInputFormat.setInputPaths(job1, aggregateFilePath);
        Path aggregateOutputPath = new Path(args[1] + "/filter/aggregate");
        FileOutputFormat.setOutputPath(job1, aggregateOutputPath);
        job1.waitForCompletion(true);

        // 2 job
        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2);
        job2.setSortComparatorClass(DecreasingTextComparator.class);
        job2.setJarByClass(Driver.class);
        job2.setMapperClass(DeathFileMapper.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setNumReduceTasks(0);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);
        Path deathFilePath = new Path(inputDirPath + "/deaths.csv");
        FileInputFormat.setInputPaths(job2, deathFilePath);
        Path deathsOutputPath = new Path(args[1] + "/filter/deaths/");
        FileOutputFormat.setOutputPath(job2, deathsOutputPath);
        job2.waitForCompletion(true);

        // 3 job
        Configuration conf3 = new Configuration();
        Job job3 = Job.getInstance(conf3);
        job3.setSortComparatorClass(DecreasingTextComparator.class);
        job3.setJarByClass(Driver.class);
        job3.setMapperClass(EqualsMapper.class);
        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(Text.class);
        job3.setReducerClass(JoinReducer.class);
        // job3.setNumReduceTasks(0);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);
        job3.setInputFormatClass(TextInputFormat.class);
        job3.setOutputFormatClass(TextOutputFormat.class);
        Path inputPath = new Path(args[1] + "/filter/");
        FileInputFormat.setInputPaths(job3, new Path(args[1] + "/filter/aggregate/"),
                new Path(args[1] + "/filter/deaths/"));
        Path outputPath = new Path(args[1] + "/join");
        FileOutputFormat.setOutputPath(job3, outputPath);
        job3.waitForCompletion(true);


        // 4 job
        Configuration conf4 = new Configuration();
        Job job4 = Job.getInstance(conf4);
        job4.setSortComparatorClass(DecreasingTextComparator.class);
        job4.setJarByClass(Driver.class);
        job4.setMapperClass(AverageMapper.class);
        job4.setMapOutputKeyClass(IntWritable.class);
        job4.setMapOutputValueClass(Text.class);
        job4.setReducerClass(AverageReducer.class);
        job4.setNumReduceTasks(1);
        job4.setOutputKeyClass(IntWritable.class);
        job4.setOutputValueClass(Text.class);
        job4.setInputFormatClass(TextInputFormat.class);
        job4.setOutputFormatClass(TextOutputFormat.class);
        Path finalInputPath = new Path(args[1] + "/join/");
        FileInputFormat.setInputPaths(job4, finalInputPath);
        Path finalOutputPath = new Path(args[1] + "/final");
        FileOutputFormat.setOutputPath(job4, finalOutputPath);
        job4.waitForCompletion(true);

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

