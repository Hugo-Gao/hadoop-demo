package Spring;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;


/**
 * 使用Spring Hadoop访问HDFS文件系统
 */
public class SpringHadoopHDFSApp
{
    private ApplicationContext ctx;
    private FileSystem fileSystem;

    @Before
    public void setUp()
    {
        ctx = new ClassPathXmlApplicationContext("beans.xml");
        fileSystem = (FileSystem) ctx.getBean("fileSystem");

    }

    @Test
    public void testMkDirs() throws Exception
    {
        fileSystem.mkdirs(new Path("/springhdfs/"));
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
     * 读取HDFS文件内容
     * @throws Exception
     */
    @Test
    public void testReadFile() throws Exception
    {
        FSDataInputStream in = fileSystem.open(new Path("/springhdfs/hello.txt"));
        IOUtils.copyBytes(in, System.out, 1024);
        in.close();
    }



    @After
    public void tearDown() throws Exception
    {
        ctx = null;
        fileSystem.close();
    }
}
