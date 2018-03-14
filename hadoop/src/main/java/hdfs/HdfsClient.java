package hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.IOException;

/**
 * Created by chongaizhen on 2018/03/13.
 */
public class HdfsClient {

    /*
    如果我们的代码中没有指定fs.defaultFS，并且工程classpath下也没有给定相应的配置，
    conf中的默认值就来自于hadoop的jar包中的core-default.xml，默认值为： file://

    参数优先级： 1、客户端代码中设置的值 2、classpath下的用户自定义配置文件 3、服务器的默认配置（jar包底下自带的xml）
     */
    public static void main(String[] args) throws IOException, InterruptedException {
        listFile2();
    }

    public static void uploadFile() throws IOException {
        Configuration configuration = new Configuration();
        // TODO: 2018/03/14 为什么设置副本数为2不起作用？是不是因为是LocalFileSystem？
//        带上参数取到的就是DistributedFileSystem（HDFS文件系统）
//        configuration.set("fs.defaultFS", "hdfs://hdp-node01:9000");

//         如果这样去获取，那conf里面就可以不要配"fs.defaultFS"参数，而且，这个客户端的身份标识已经是hadoop用户
//        fileSystem = FileSystem.get(new URI("hdfs://hdp-node01:9000"), conf, "hadoop");

        configuration.set("dfs.replication","2");
        FileSystem fileSystem = FileSystem.get(configuration);
        Path localPath = new Path("E:\\IdeaProjects\\git\\BigData\\hadoop\\src\\main\\java\\hdfs\\test");
        Path hdfsPath = new Path("E:\\IdeaProjects\\git\\BigData\\hadoop\\src\\main\\java\\hdfs\\testHdfs\\uploadFile");
        fileSystem.copyFromLocalFile(localPath, hdfsPath);
        fileSystem.close();
    }

    public static void uploadFile2() throws IOException {
        Configuration configuration = new Configuration();
        // TODO: 2018/03/14 为什么设置副本数为2不起作用？是不是因为是LocalFileSystem？
        configuration.set("dfs.replication","2");
        FileSystem fileSystem = FileSystem.get(configuration);
        fileSystem.close();
    }

    public static void downloadFile() throws IOException {
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(configuration);
        Path hdfsPath = new Path("E:\\IdeaProjects\\git\\BigData\\hadoop\\src\\main\\java\\hdfs\\testHdfs\\uploadFile2");
        Path localPath = new Path("E:\\IdeaProjects\\git\\BigData\\hadoop\\src\\main\\java\\hdfs\\testDownload");
        fileSystem.copyToLocalFile(hdfsPath, localPath);
        fileSystem.close();
    }

    public static void downloadFile2() throws IOException {
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(configuration);
        fileSystem.close();
    }

    public static void listFile() throws IOException {

        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(configuration);

//        因为List是内存集合，假如文件数量足够多的话会占用大量的内存，所以返回迭代器
        RemoteIterator<LocatedFileStatus> listFiles = fileSystem.listFiles(new Path("E:\\IdeaProjects\\git\\BigData\\hadoop\\src\\main\\java\\hdfs"), true);

        while (listFiles.hasNext()) {
            LocatedFileStatus fileStatus = listFiles.next();
            System.out.println(fileStatus.getPath().getName());
            System.out.println(fileStatus.getBlockSize());
            System.out.println(fileStatus.getPermission());
            System.out.println(fileStatus.getLen());
            BlockLocation[] blockLocations = fileStatus.getBlockLocations();
            for (BlockLocation bl : blockLocations) {
                //获取文件块的大小和编号
                System.out.println("block-length:" + bl.getLength() + "--" + "block-offset:" + bl.getOffset());
                //获取该文件块保存在哪个节点
                String[] hosts = bl.getHosts();
                for (String host : hosts) {
                    System.out.println(host);
                }
            }
            System.out.println("--------------分割线--------------");
        }
    }

    public static void listFile2() throws IOException {

        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(configuration);

        //不会进行递归遍历
        FileStatus[] listStatus = fileSystem.listStatus(new Path("E:\\IdeaProjects\\git\\BigData\\hadoop\\src\\main\\java\\hdfs"));

        for (FileStatus fstatus : listStatus) {
            String flag = "d-- ";
            if (fstatus.isFile()) flag = "f-- ";
            System.out.println(flag + fstatus.getPath().getName());
        }
    }

    public static void mkdirs() throws IOException {
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(configuration);
        fileSystem.mkdirs(new Path("E:\\IdeaProjects\\git\\BigData\\hadoop\\src\\main\\java\\hdfs\\testHdfs\\test1"));
        fileSystem.close();
    }

    public static void createFile() throws IOException {
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(configuration);
        fileSystem.create(new Path("E:\\IdeaProjects\\git\\BigData\\hadoop\\src\\main\\java\\hdfs\\testHdfs\\test1\\test2"));
        fileSystem.close();
    }

    public static void rename() throws IOException {
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(configuration);
        Path oldPath = new Path("E:\\IdeaProjects\\git\\BigData\\hadoop\\src\\main\\java\\hdfs\\testHdfs\\test1\\test2");
        Path newPath = new Path("E:\\IdeaProjects\\git\\BigData\\hadoop\\src\\main\\java\\hdfs\\testHdfs\\test1\\testRename");
        fileSystem.rename(oldPath, newPath);
        fileSystem.close();
    }

    public static void delFile() throws IOException {
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(configuration);
        // 删除文件夹 ，如果是非空文件夹，参数2必须给值true
        fileSystem.delete(new Path("E:\\IdeaProjects\\git\\BigData\\hadoop\\src\\main\\java\\hdfs\\testHdfs\\test1"),true);
        fileSystem.close();
    }
}
