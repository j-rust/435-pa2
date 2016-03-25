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

    public static final String MYSTERY_AUTHOR = "Mystery Author";

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
//        Configuration countConf = new Configuration();
//        Job count = Job.getInstance(countConf, "main");
//        count.setJobName("Author Count");
//        count.setJarByClass(Main.class);
//
//        count.setOutputKeyClass(Text.class);
//        count.setOutputValueClass(Text.class);
//
//        count.setMapperClass(AuthorCountMap.class);
//        count.setReducerClass(AuthorCountReduce.class);
//
//        FileInputFormat.setInputPaths(count, new Path(args[0]));
//        FileOutputFormat.setOutputPath(count, new Path("/tmp/out/authorCount"));
//
//        count.waitForCompletion(true);
//
//        long authors = count.getCounters().findCounter(Counters.TOTAL_AUTHORS).getValue();
//        System.out.println("**********AUTHORS: " + authors);
//
//        cleanup(count.getConfiguration(), "/tmp/out/authorCount");


        /* Term Frequency job */
//        Configuration tfConf = new Configuration();
//        tfConf.set("authors", authors + "");
//
//        Job tf = Job.getInstance(tfConf, "main");
//        tf.setJobName("TF");
//
//        tf.setJarByClass(Main.class);
//
//        tf.setOutputKeyClass(Text.class);
//        tf.setOutputValueClass(Text.class);
//
//        tf.setReducerClass(TFReduce.class);
//        tf.setMapperClass(TFMap.class);
//
//        FileInputFormat.setInputPaths(tf, new Path(args[0]));
//        FileOutputFormat.setOutputPath(tf, new Path("/tmp/out/tfOut"));
//
//        tf.waitForCompletion(true);

        /* Inverted Document Frequency job */
//        Configuration idfConf = new Configuration();
//        idfConf.set("authors", authors + "");
//
//        Job idf = Job.getInstance(idfConf, "main");
//        idf.setJobName("IDF");
//
//        idf.setJarByClass(Main.class);
//
//        idf.setOutputKeyClass(Text.class);
//        idf.setOutputKeyClass(Text.class);
//
//        idf.setMapperClass(IDFMap.class);
//        idf.setReducerClass(IDFReduce.class);
//
//        FileInputFormat.setInputPaths(idf, new Path("/tmp/out/tfOut"));
//        FileOutputFormat.setOutputPath(idf, new Path("/tmp/out/tfidfOut"));
//
//        idf.waitForCompletion(true);
//        cleanup(idfConf, "/tmp/out/tfOut");
//

        /* Build author attribute vectors */
//        Configuration aavConf = new Configuration();
//        Job aav = Job.getInstance(aavConf, "main");
//
//        aav.setJobName("AAV");
//
//        aav.setJarByClass(Main.class);
//
//        aav.setOutputKeyClass(Text.class);
//        aav.setOutputValueClass(Text.class);
//
//        aav.setMapperClass(AAVMap.class);
//        aav.setReducerClass(AAVReduce.class);
//
//        FileInputFormat.setInputPaths(aav, new Path("/tmp/out/tfidfOut"));
//        FileOutputFormat.setOutputPath(aav, new Path("/tmp/out/allAav"));
//
//        aav.waitForCompletion(true);

        /* cache original tfidf values for use in author detection */
//        Configuration cacheConf = new Configuration();
//        Job cache = Job.getInstance(cacheConf, "main");
//
//        cache.setJobName("TFIDF Cache");
//
//        cache.setJarByClass(Main.class);
//
//        cache.setOutputValueClass(Text.class);
//        cache.setOutputKeyClass(Text.class);
//
//        cache.setMapperClass(IDFCacheMap.class);
//        cache.setReducerClass(IDFCacheReduce.class);
//
//        FileInputFormat.setInputPaths(cache, new Path("/tmp/out/tfidfOut"));
//        FileOutputFormat.setOutputPath(cache, new Path("/tmp/out/cacheOut"));
//
//        cache.waitForCompletion(true);


        /* START OF AUTHOR DETECTION - PART 2 OF ASSIGNMENT */

        /* calculate tf for mystery document */
        Configuration mystTFConf = new Configuration();
        Job mystTf = Job.getInstance(mystTFConf, "main");

        mystTf.setJobName("Mystery TF");

        mystTf.setJarByClass(Main.class);

        mystTf.setOutputKeyClass(Text.class);
        mystTf.setOutputValueClass(Text.class);

        mystTf.setMapperClass(MysteryTFMap.class);
        mystTf.setReducerClass(TFReduce.class);

        FileInputFormat.setInputPaths(mystTf, new Path(args[0]));
        FileOutputFormat.setOutputPath(mystTf, new Path("/tmp/out/mysteryTfOut"));

        mystTf.waitForCompletion(true);


        /* calculate tfidf values for mystery document */
        Configuration mystIdfConf = new Configuration();
        Job mystIdf = Job.getInstance(mystIdfConf, "main");

        mystIdf.setJobName("Mystery IDF");

        mystIdf.setJarByClass(Main.class);

        mystIdf.setOutputKeyClass(Text.class);
        mystIdf.setOutputValueClass(Text.class);

        mystIdf.setMapperClass(MysteryIDFMap.class);
        mystIdf.setReducerClass(MysteryIDFReduce.class);

        mystIdf.setNumReduceTasks(200);

        mystIdf.addCacheFile(new Path("/tmp/out/cacheOut/part-r-00000").toUri());

        FileInputFormat.setInputPaths(mystIdf, new Path("/tmp/out/mysteryTfOut"));
        FileOutputFormat.setOutputPath(mystIdf, new Path("/tmp/out/mysteryTfidfOut"));

        mystIdf.waitForCompletion(true);

        /* Build AAV for mystery document */
        Configuration mystAAVConf = new Configuration();
        Job mystAAV = Job.getInstance(mystAAVConf, "main");

        mystAAV.setJobName("AAV");

        mystAAV.setJarByClass(Main.class);

        mystAAV.setOutputKeyClass(Text.class);
        mystAAV.setOutputValueClass(Text.class);

        mystAAV.setMapperClass(AAVMap.class);
        mystAAV.setReducerClass(AAVReduce.class);

        FileInputFormat.setInputPaths(mystAAV, new Path("/tmp/out/mysteryTfidfOut"));
        FileOutputFormat.setOutputPath(mystAAV, new Path("/tmp/out/mysteryAavOut"));

        mystAAV.waitForCompletion(true);



        /* copy mystery doc AAV to common directory */
        FileSystem fs = FileSystem.get(mystAAVConf);
        fs.rename(new Path("/tmp/out/mysteryAavOut/part-r-00000"), new Path("/tmp/out/allAav/mysteryAav"));


        /* Calculate cosine similarity */
        Configuration cosSimConf = new Configuration();
        Job cosSim = Job.getInstance(cosSimConf, "main");

        cosSim.setJobName("Cosine Similarity");

        cosSim.setJarByClass(Main.class);

        cosSim.setOutputValueClass(Text.class);
        cosSim.setOutputKeyClass(Text.class);

        cosSim.setMapperClass(CosineSimMap.class);
        cosSim.setReducerClass(CosineSimReduce.class);

        FileInputFormat.setInputPaths(cosSim, new Path("/tmp/out/allAav"));
        FileOutputFormat.setOutputPath(cosSim, new Path("/tmp/out/cosSimOut"));

        cosSim.waitForCompletion(true);

        /* Find 10 highest cosine similarity values */
        Configuration sortConf = new Configuration();
        Job sort = Job.getInstance(sortConf, "main");

        sort.setJobName("Sort");

        sort.setJarByClass(Main.class);

        sort.setOutputValueClass(Text.class);
        sort.setOutputKeyClass(Text.class);

        sort.setMapperClass(SimilaritySortMap.class);
        sort.setCombinerClass(SimilaritySortCombiner.class);
        sort.setReducerClass(SimilaritySortReduce.class);

        FileInputFormat.setInputPaths(sort, new Path("/tmp/out/cosSimOut"));
        FileOutputFormat.setOutputPath(sort, new Path(args[1]));

        sort.waitForCompletion(true);
    }
}
