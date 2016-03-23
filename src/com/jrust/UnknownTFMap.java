package com.jrust;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by Jonathan Rust on 3/23/16.
 */
public class UnknownTFMap extends Mapper<LongWritable, Text, Text, Text> {


    /**
     * Parses the document with unknown author
     * Outputs <"unknown", term>
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

    }

}

