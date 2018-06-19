package Spring;


import org.apache.hadoop.fs.FileStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.data.hadoop.fs.FsShell;

import java.util.Collection;

@SpringBootApplication
public class SpringBootHDFSApp implements CommandLineRunner
{
    @Autowired
    FsShell fsShell;

    @Override
    public void run(String... strings)
    {

        Collection<FileStatus> fileStatuses = fsShell.ls("/springhdfs");
        System.out.println("hello");
        System.out.println(fileStatuses.size());

        for (FileStatus fileStatus : fileStatuses)
        {
            System.out.println(">" + fileStatus.getPath());
        }

    }

    public static void main(String[] args)
    {
        SpringApplication.run(SpringBootHDFSApp.class, args);
    }
}
