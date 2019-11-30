package com.demo10086.demo.controller;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.elasticsearch.hadoop.mr.EsOutputFormat;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;

@RestController
@Api(description = "测试模块")
@RequestMapping("/test")
public class TestController {

    @ApiOperation(value = "GET请求")
    @GetMapping("/generateData/{value}")
    public String dynamicBuildData(@PathVariable(name = "value") String value) {
        return "input value : " + value;
    }

    @ApiOperation(value = "GET请求")
    @GetMapping("/json2es")
    public String json2es() {
        try {
            test();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return "input value : " ;
    }


    public void test() throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.setBoolean("mapred.map.tasks.speculative.execution", false);
        conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);
        conf.set("es.nodes", "localhost:9200");
        //设置索引/类型
        conf.set("es.resource", "test/test");
        conf.set("es.mapping.id", "id");
        conf.set("es.input.json", "yes");
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());

        Job job = Job.getInstance(conf, "hadoop es write test");
        job.setMapperClass(TestHDFSToEs.MyMapper.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(EsOutputFormat.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);

        // 设置输入路径
        FileInputFormat.setInputPaths(job, new Path
                ("hdfs://localhost:9000/work"));
        job.waitForCompletion(true);
    }
}
