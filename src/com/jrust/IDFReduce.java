package com.jrust;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;

/**
 * Created by Jonathan Rust on 3/8/16.
 */
public class IDFReduce extends Reducer<Text, Text, Text, Text> {

    @Override
    /**
     * @args key - term
     * @args values - [Author_0 + TF_0, Author_1 + TF_1, ..., Author_N + TF_N
     */
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        long total_authors = Long.parseLong(conf.get("authors"));
        HashSet<String> authors = new HashSet<>();
        ArrayList<String> values_list = new ArrayList<>();

        for (Text t : values) {
            String[] t_split = t.toString().split("\\s+");
            String author = t_split[0];
            String tf = t_split[1];
            if(!author.equals("")) {
                authors.add(author);
                values_list.add(author + " " + tf);
            }
        }
        Integer author_count = authors.size();
        Double idf = Math.log((double)total_authors / (double)author_count);

        for (String s : values_list) {
            String[] split = s.split(" ");
            String author = split[0];
            Double tf = Double.parseDouble(split[1]);
            Double tfidf = tf*idf;
            context.write(new Text(author), new Text(tfidf.toString()));
        }

    }
}
