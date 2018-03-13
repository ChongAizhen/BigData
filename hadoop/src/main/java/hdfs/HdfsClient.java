package hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * Created by chongaizhen on 2018/03/13.
 */
public class HdfsClient {

    static FileSystem fileSystem = null;

    public static void main(String[] args) throws IOException {
        init();
        uploadFile();
    }

    public static void init() throws IOException {
        Configuration configuration = new Configuration();
        fileSystem = FileSystem.get(configuration);
    }

    public static void uploadFile() throws IOException {
        fileSystem.copyFromLocalFile(new Path("E:\\IdeaProjects\\git\\BigData\\hadoop\\src\\main\\java\\hdfs\\test"),new Path("E:\\IdeaProjects\\git\\BigData\\hadoop\\src\\main\\java\\hdfs\\testHdfs\\test"));
    }

}
