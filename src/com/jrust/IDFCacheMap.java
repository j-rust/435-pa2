package com.jrust;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by Jonathan Rust on 3/23/16.
 */
public class IDFCacheMap extends Mapper<LongWritable, Text, Text, Text> {

    /**
     * Method used solely to cache tfidf values for use later on in author detection
     * Input is a line from IDFReduce output
     * @param key
     * @param value
     * @param context
     */
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split("\t");
        String term = split[1];
        String idf = split[3];

        context.write(new Text(term), new Text(idf));
    }
}
