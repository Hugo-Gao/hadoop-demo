package com.gyf.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URI;


/**
 * Hadoop HDFS Java API 操作
 */
public class HDFSApp
{
    public static final String HDFS_PATH = "hdfs://gyfserver:9000";
    private FileSystem  fileSystem = null;
    private Configuration configuration;


    /**
     * 创建HDFS目录
     * @throws Exception
     */
    @Test
    public void mkdir() throws Exception
    {
        fileSystem.mkdirs(new Path("/hdfsapi/test"));
    }

    /**
     * 查看HDFS文件的内容
     * @throws Exception
     */
    @Test
    public void cat() throws Exception
    {
        FSDataInputStream inputStream = fileSystem.open(new Path("/springhdfs/hello.txt"));
        IOUtils.copyBytes(inputStream, System.out, 1024);
        inputStream.close();
    }


    /**
     * 重命名
     * @throws Exception
     */
    @Test
    public void rename() throws Exception
    {
        boolean rename = fileSystem.rename(new Path("/hdfsapi/test/a.txt"), new Path("/hdfsapi/test/a_new.txt"));

    }

    /**
     * 上传文件到HDFS
     * @throws Exception
     */
    @Test
    public void copyFromLocalFile() throws Exception
    {
        fileSystem.copyFromLocalFile(new Path("/Users/gaoyunfan/Desktop/fromLocal.log"), new Path("/hdfsapi/test/fromLocal.txt"));

    }


    /**
     * 上传文件到HDFS
     * @throws Exception
     */
    @Test
    public void copyToLocalFile() throws Exception
    {
        fileSystem.copyToLocalFile( new Path("/hdfsapi/test/fromLocal.txt"),new Path("/Users/gaoyunfan/Desktop/fromLocal2.log"));

    }

    /**
     * 查看某个目录下所有文件
     * @throws Exception
     */
    @Test
    public void listFiles() throws Exception
    {
        FileStatus[] fileStatuses = fileSystem.listStatus(new Path("/hdfsapi/test"));
        for (FileStatus fileStatus : fileStatuses)
        {
            String isDir = fileStatus.isDirectory() ? "文件夹" : "文件";
            short replication = fileStatus.getReplication();
            long len = fileStatus.getLen();
            String path = fileStatus.getPath().toString();
            System.out.println(isDir + "\t" + replication + "\t" + len + "\t" + path);
        }

    }

    /**
     * 删除文件
     * @throws Exception
     */
    @Test
    public void delete() throws Exception
    {
        fileSystem.delete(new Path("/hdfsapi/test/spark.tgz"),true);
    }

    @Test
    public void copyFromLocalFileWithProgress() throws Exception
    {
        InputStream in = new BufferedInputStream(new FileInputStream(new File("/Users/gaoyunfan/spark-2.3.0-bin-hadoop2.7.tgz")));
        FSDataOutputStream output = fileSystem.create(new Path("/hdfsapi/test/spark.tgz"), new Progressable()
        {
            @Override
            public void progress()
            {
                System.out.print(".");//带进度提醒信息
            }
        });
        IOUtils.copyBytes(in, output,4096);
    }
    /**
     * 创建文件
     * @throws Exception
     */
    @Test
    public void create() throws Exception
    {
        FSDataOutputStream output = fileSystem.create(new Path("/hdfsapi/test/c.txt"));
        output.write("Hello I'm Hugo".getBytes());
        output.flush();
        output.close();
    }

    @Before
     public void setUp() throws Exception
     {
         System.out.println("HDFSApp.setUp");
         configuration = new Configuration();
         configuration.set("dfs.client.use.datanode.hostname","true");
         fileSystem = FileSystem.get(new URI(HDFS_PATH), configuration,"root");
     }


     @After
     public void tearDown() throws Exception
     {
         configuration = null;
         fileSystem = null;
         System.out.println("HDFSApp.tearDown");
     }
}
