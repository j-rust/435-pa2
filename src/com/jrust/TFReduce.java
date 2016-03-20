package com.jrust;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;

/**
 * Created by Jonathan Rust on 3/2/16.
 */
public class TFReduce extends Reducer<Text, Iterable, Text, Text> {


    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        Configuration conf = context.getConfiguration();
//        long total_authors = Long.parseLong(conf.get("authors"));
        HashMap<String, Integer> term_count = new HashMap<>();
        Integer max_count = 0;
        for(Text term : values){
            if (!term_count.containsKey(term.toString())) {
                term_count.put(term.toString(), 1);
            } else {
                Integer new_val = term_count.get(term.toString()) + 1;
                term_count.replace(term.toString(), new_val);

                if(new_val > max_count) max_count = new_val;
            }
        }

        for (HashMap.Entry<String, Integer> entry : term_count.entrySet()) {
            String term = entry.getKey();
            Integer count = entry.getValue();
            double tf = (double) count / (double) max_count;
            String value_string = term + "\t" + tf;
            context.write(key, new Text(value_string));
        }

    }

}
