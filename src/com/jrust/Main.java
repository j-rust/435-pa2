package com.jrust;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


public class Main {

    public enum Counters{
        TOTAL_AUTHORS
    }

    public static void cleanup(Configuration conf, String dir_path) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        if(fs.exists(new Path(dir_path))){
            fs.delete(new Path(dir_path),true);
        }
    }

    public static void main(String[] args) throws Exception {

        /* Author counting job */
        Configuration countConf = new Configuration();
        Job count = Job.getInstance(countConf, "main");
        count.setJobName("Author Count");
        count.setJarByClass(Main.class);

        count.setMapperClass(AuthorCountMap.class);
        count.setReducerClass(AuthorCountReduce.class);

        FileInputFormat.setInputPaths(count, new Path(args[0]));
        FileOutputFormat.setOutputPath(count, new Path("/tmp/out/authorCount"));

        count.waitForCompletion(true);

        long authors = count.getCounters().findCounter(Counters.TOTAL_AUTHORS).getValue();

        cleanup(count.getConfiguration(), "/tmp/out/authorCount");

        /* Term Frequency job */
        Configuration tfConf = new Configuration();
//        tfConf.set("authors", authors + "");

        Job tf = Job.getInstance(tfConf, "main");
        tf.setJobName("TF");

        tf.setJarByClass(Main.class);

        tf.setOutputKeyClass(Text.class);
        tf.setOutputValueClass(Text.class);

        tf.setReducerClass(TFReduce.class);
        tf.setMapperClass(TFMap.class);

        FileInputFormat.setInputPaths(tf, new Path(args[0]));
        FileOutputFormat.setOutputPath(tf, new Path(args[1]));
//        FileOutputFormat.setOutputPath(tf, new Path("/tmp/out/tfOut"));

        tf.waitForCompletion(true);

        /* Inverted Document Frequency job */
        Configuration idfConf = new Configuration();
        idfConf.set("authors", authors + "");

        Job idf = Job.getInstance(idfConf, "main");
        idf.setJobName("IDF");

        idf.setJarByClass(Main.class);

        idf.setOutputKeyClass(Text.class);
        idf.setOutputKeyClass(Text.class);

        idf.setMapperClass(IDFMap.class);
        idf.setReducerClass(IDFReduce.class);

        FileInputFormat.setInputPaths(idf, new Path("/tmp/out/tfOut"));
        FileOutputFormat.setOutputPath(idf, new Path(args[1]));

        idf.waitForCompletion(true);
        cleanup(idfConf, "/tmp/out/tfOut");
    }
}
