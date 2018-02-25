package edu.bu.metcs.HadoopEx;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Task2 extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new Task2(), args);
        System.exit(res);
	}

    public int run(String args[]) {
        try {
            Configuration conf = new Configuration();

            Job job = new Job(conf, "Task2");
            job.setJarByClass(Task2.class);

            // specify a Mapper
            job.setMapperClass(Task2Mapper.class);

            // specify a Reducer
            job.setReducerClass(Task2Reducer.class);

            // specify output types
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);

            // specify input and output directories  
            FileInputFormat.addInputPath(job, new Path(args[0]));
            job.setInputFormatClass(TextInputFormat.class);

            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            job.setOutputFormatClass(TextOutputFormat.class);

            return(job.waitForCompletion(true) ? 0 : 1);
        } catch (InterruptedException|ClassNotFoundException|IOException e) {
            System.err.println("Error during mapreduce job.");
            e.printStackTrace();
            return 2;
        }
    }
}
