package com.demo10086.demo.controller;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.elasticsearch.hadoop.mr.EsOutputFormat;
import java.io.IOException;

/**
 * Created by bee on 2019-07-18.
 * @Author WS
 */
public class TestHDFSToEs {

    public static class MyMapper extends Mapper<Object, Text, NullWritable, Text> {
        private Text line = new Text();
        public void map(Object key, Text value, Mapper<Object, Text, NullWritable, Text>.Context context) throws IOException, InterruptedException {

            if(value.getLength()>0){
                line.set(value);
                context.write(NullWritable.get(), line);
            }

        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

//        Configuration conf = new Configuration();
//        conf.setBoolean("mapred.map.tasks.speculative.execution", false);
//        conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);
//        conf.set("es.nodes", "localhost:9200");
//        //设置索引/类型
//        conf.set("es.resource", "test/test");
//        conf.set("es.mapping.id", "id");
//        conf.set("es.input.json", "yes");
//
//        Job job = Job.getInstance(conf, "hadoop es write test");
//        job.setMapperClass(TestHDFSToEs.MyMapper.class);
//        job.setInputFormatClass(TextInputFormat.class);
//        job.setOutputFormatClass(EsOutputFormat.class);
//        job.setMapOutputKeyClass(NullWritable.class);
//        job.setMapOutputValueClass(Text.class);
//
//        // 设置输入路径
//        FileInputFormat.setInputPaths(job, new Path
//                ("hdfs://49.233.208.207:9000/work"));
//        job.waitForCompletion(true);
    }

//    public void test()  throws IOException, ClassNotFoundException, InterruptedException {
//        Configuration conf = new Configuration();
//        conf.setBoolean("mapred.map.tasks.speculative.execution", false);
//        conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);
//        conf.set("es.nodes", "localhost:9200");
//        //设置索引/类型
//        conf.set("es.resource", "test/test");
//        conf.set("es.mapping.id", "id");
//        conf.set("es.input.json", "yes");
//
//        Job job = Job.getInstance(conf, "hadoop es write test");
//        job.setMapperClass(TestHDFSToEs.MyMapper.class);
//        job.setInputFormatClass(TextInputFormat.class);
//        job.setOutputFormatClass(EsOutputFormat.class);
//        job.setMapOutputKeyClass(NullWritable.class);
//        job.setMapOutputValueClass(Text.class);
//
//        // 设置输入路径
//        FileInputFormat.setInputPaths(job, new Path
//                ("hdfs://localhost:9000/work"));
//        job.waitForCompletion(true);
//    }
}

