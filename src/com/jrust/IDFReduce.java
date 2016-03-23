package com.jrust;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.DecimalFormat;
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
            String[] t_split = t.toString().split("\t");
            String author = t_split[0];
            String tf = t_split[1];
            if(!author.equals("")) {
                authors.add(author);
                values_list.add(author + "\t" + tf);
            }
        }
        Integer author_count = authors.size();
        Double idf = Math.log((double)total_authors / (double)author_count) / Math.log(2.0);

        for (String s : values_list) {
            String[] split = s.split("\t");
            String author = split[0];
            double tf = Double.parseDouble(split[1]);
            double tfidf = tf*idf;
            DecimalFormat df = new DecimalFormat("0.000000000000000");
            context.write(new Text(author), new Text(key.toString() + "\t" +
                    df.format(tfidf)));
        }

    }
}
