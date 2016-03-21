package com.jrust;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by Jonathan Rust on 3/8/16.
 */
public class IDFMap extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] split = value.toString().split("\\s");

        String author = split[0];
        String term   = split[1];
        String tf     = split[2];

        context.write(new Text(term), new Text(author + " " + tf));
    }
}
