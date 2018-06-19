package com.gyf.hadoop.project;

import com.kumkee.userAgent.UserAgent;
import com.kumkee.userAgent.UserAgentParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class LogApp
{
    /**
     * map:读取输入的文件
     */
    public static class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable>
    {
        LongWritable one = new LongWritable(1);
        private UserAgentParser userAgentParser;
        @Override
        protected void setup(Context context) throws IOException, InterruptedException
        {
            userAgentParser = new UserAgentParser();

        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
        {
            //每一行日志信息
            String line = value.toString();

            String source = line.substring(getCharacterPosition(line, "\"", 7) + 1, getCharacterPosition(line, "\"", 8));
            UserAgent agent = userAgentParser.parse(source);
            String browser = agent.getBrowser();
            context.write(new Text(browser), one);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException
        {
            userAgentParser = null;
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
     * 获取指定字符串中指定表示的字符串出现的索引位置
     *
     * @param value
     * @param operator
     * @param index
     * @return
     */
    private static int getCharacterPosition(String value, String operator, int index)
    {
        Matcher slashMatcher = Pattern.compile(operator).matcher(value);
        int mIdx = 0;
        while (slashMatcher.find())
        {
            mIdx++;
            if (mIdx == index)
            {
                break;
            }
        }
        return slashMatcher.start();
    }
    /**
     * 定义Driver,封装了MapReduce的所有作业信息
     */
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException
    {
        Configuration conf = new Configuration();

//        准备清理已存在的目录
        Path outPath = new Path(args[1]);
        FileSystem fileSystem = FileSystem.get(conf);
        if (fileSystem.exists(outPath))
        {
            fileSystem.delete(outPath, true);
            System.out.println("output file exists, but it has deleted");

        }


        Job job = Job.getInstance(conf, "LogApp");
        //设置job的处理类
        job.setJarByClass(LogApp.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        //设置Map相关参数
        job.setMapperClass(LogApp.MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        //设置Reducer相关参数
        job.setReducerClass(LogApp.MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        //设置输出
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.out.println(job.waitForCompletion(true)?0:1);
    }
}
