package com.gyf.hadoop.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.FileOutputStream;
import java.io.IOException;

/**
 * 使用MapReduce开发WordCount应用程序
 */
public class WordCountApp
{
    /**
     * map:读取输入的文件
     */
    public static class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable>
    {
        LongWritable one = new LongWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
        {
            String line = value.toString();
            //将每一行拆分单词
            String[] words = line.split(" ");
            for (String word : words)
            {
                //通过上下文将map输出
                context.write(new Text(word),one);
            }
        }
    }


    /**
     * Reduce:归并操作
     */
    public static class MyReducer extends Reducer<Text, LongWritable, Text, LongWritable>
    {
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException
        {
            long sum = 0;
            for (LongWritable value : values)
            {
                //求单词key出现的总和
                sum += value.get();
            }
            context.write(key, new LongWritable(sum));
        }
    }

    /**
     * 定义Driver,封装了MapReduce的所有作业信息
     */
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException
    {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "wordcount");
        //设置job的处理类
        job.setJarByClass(WordCountApp.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        //设置Map相关参数
        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        //设置Reducer相关参数
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        //设置输出
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.out.println(job.waitForCompletion(true)?0:1);
    }
}
