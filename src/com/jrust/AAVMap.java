package com.jrust;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by Jonathan Rust on 3/21/16.
 */
public class AAVMap extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
        String[] split = value.toString().split("\\s+");
        String author = split[0];
        String tfidf  = split[1];

        context.write(new Text(author), new Text(tfidf));
    }
}
