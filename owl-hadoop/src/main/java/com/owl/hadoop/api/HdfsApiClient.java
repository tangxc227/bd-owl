package com.owl.hadoop.api;

import com.owl.hadoop.util.HdfsUtil;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;

/**
 * @Author: tangxc
 * @Description:
 * @Date: Created in 09:50 2018/12/18
 * @Modified by:
 */
public class HdfsApiClient {

    private static FileSystem fileSystem = null;

    static {
        try {
            fileSystem = HdfsUtil.getFileSystem();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public static void main(String[] args) throws IOException {
        try {
//            createNewFile();
//            append();
//            copyFromLocal();
//            copyToLocal();
//            create();
//            mkdir();
//            delete();
//            read();
            getFileStatus();
        } finally {
            if (null != fileSystem) {
                fileSystem.close();
            }
        }

    }

    /**
     * 在HDFS上创建新文件
     *
     * @throws IOException
     */
    public static void createNewFile() throws IOException {
        boolean created = fileSystem.createNewFile(new Path("/hadoop/api/createNewFile.txt"));
        System.out.println(created);
    }

    /**
     * 往HDFS上空文件追加内容
     *
     * @throws IOException
     */
    public static void append() throws IOException {
        Path path = new Path("/hadoop/api/createNewFile.txt");
        FSDataOutputStream outputStream = fileSystem.append(path);
        outputStream.write("大数据".getBytes());
        outputStream.close();
    }

    /**
     * 上传本地文件到HDFS
     *
     * @throws IOException
     */
    public static void copyFromLocal() throws IOException {
        fileSystem.copyFromLocalFile(new Path("datas/1.txt"), new Path("/hadoop/api/2.txt"));
    }

    /**
     * COPY HDFS文件到本地
     *
     * @throws IOException
     */
    public static void copyToLocal() throws IOException {
        fileSystem.copyToLocalFile(new Path("/hadoop/api/2.txt"), new Path("datas/"));
    }

    /**
     * 创建文件并写入内容
     *
     * @throws IOException
     */
    public static void create() throws IOException {
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fileSystem.create(new Path("/hadoop/api/3.txt"), (short) 1)));
        writer.write("大数据");
        writer.newLine();
        writer.write("离线数据分析平台");
        writer.close();
    }

    /**
     * 创建文件夹
     *
     * @throws IOException
     */
    public static void mkdir() throws IOException {
        boolean created = fileSystem.mkdirs(new Path("/hadoop/api/newDir"));
        System.out.println(created ? "创建成功" : "创建失败");
    }

    /**
     * 删除HDFS文件、文件夹
     *
     * @throws IOException
     */
    public static void delete() throws IOException {
        boolean delete = fileSystem.delete(new Path("/hadoop/api/createNewFile.txt"), true);
        System.out.println(delete ? "删除成功" : "删除失败");
    }

    /**
     * 读取HDFS文件
     *
     * @throws IOException
     */
    public static void read() throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(fileSystem.open(new Path("/hadoop/api/2.txt"))));
        String line;
        while ((line = reader.readLine()) != null) {
            System.out.println(line);
        }
    }

    /**
     * 获取文件状态信息
     *
     * @throws IOException
     */
    public static void getFileStatus() throws IOException {
        FileStatus status = fileSystem.getFileStatus(new Path("/hadoop/api/2.txt"));
        System.out.println(status.isDirectory() ? "是文件夹" : "是文件");
        System.out.println("提交时间:" + status.getAccessTime());
        System.out.println("复制因子:" + status.getReplication());
        System.out.println("长度:" + status.getLen());
        System.out.println("最后修改时间:" + status.getModificationTime());
    }

}
