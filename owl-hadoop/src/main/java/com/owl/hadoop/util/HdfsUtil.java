package com.owl.hadoop.util;

import com.owl.hadoop.configuration.ConfigurationManager;
import com.owl.hadoop.constant.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * @Author: tangxc
 * @Description:
 * @Date: Created in 09:40 2018/12/18
 * @Modified by:
 */
public class HdfsUtil {

    /**
     * 获取configuration对象
     *
     * @return
     */
    public static Configuration getConfiguration() {
        Configuration configuration = new Configuration();
        configuration.set(Constants.FS_DEFAULTFS,
                ConfigurationManager.getProperty(Constants.FS_DEFAULTFS));
        return configuration;
    }

    /**
     * 获取默认FileSystem对象
     *
     * @return
     * @throws IOException
     */
    public static FileSystem getFileSystem() throws IOException {
        return getFileSystem(getConfiguration());
    }

    /**
     * 获取FileSystem对象
     *
     * @param configuration
     * @return
     * @throws IOException
     */
    public static FileSystem getFileSystem(Configuration configuration) throws IOException {
        return FileSystem.get(configuration);
    }

    /**
     * 删除HDFS文件
     *
     * @param configuration
     * @param path
     */
    public static void delete(Configuration configuration, String path) {
        boolean flag = false;
        FileSystem fileSystem = null;
        try {
            Path hdfsPath = new Path(path);
            fileSystem = getFileSystem(configuration);
            if (fileSystem.exists(hdfsPath)) {
                flag = fileSystem.delete(hdfsPath, true);
                if (!flag) {
                    throw new RuntimeException("删除HDFS文件失败：" + path);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("删除HDFS文件失败：" + path);
        }

    }

}
