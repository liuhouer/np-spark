package cn.northpark.javaSpark.lagouAPP;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;


import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;

public class HdfsClientDemo {

    FileSystem fs = null;
    Configuration configuration = null;

    @Before
    public void init() throws URISyntaxException, IOException, InterruptedException {
        // 1 获取Hadoop集群的configuration对象
        configuration = new Configuration();
//        configuration.set("fs.defaultFS", "hdfs://linux121:9000");
        // 修改默认上传的副本数量
//        configuration.set("dfs.replication", "2");

        // 2. 根据configuration获取Filesystem对象
//         fs = FileSystem.get(configuration);
        fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "haogang");
    }

    @After
    public void destroy() throws IOException {
        // 4 释放FileSystem对象
        fs.close();
    }

    //创建目录
    @Test
    public void testMkdirs() throws IOException, InterruptedException, URISyntaxException {
        // 3 使用FileSystem对象创建一个测试目录
        fs.mkdirs(new Path("/api_test2"));
    }

    //上传文件
    @Test
    public void testUpload() throws IOException, InterruptedException, URISyntaxException {
        // 4. 上传文件
        // 如果未指定副本数量，则默认是3；
        // 如果要修改副本数量，方法1：configuration对象中指定
        fs.copyFromLocalFile(new Path("E:/code/lagou.txt"), new Path("/lagou.txt"));


    }

    //下载文件
    @Test
    public void testDownload() throws IOException, InterruptedException, URISyntaxException {
        // 5. 下载文件
        fs.copyToLocalFile(true, new Path("/lagou.txt"), new Path("E:/code/lagou.txt"));

    }

    //删除文件或文件夹
    @Test
    public void testDelete() throws IOException, InterruptedException, URISyntaxException {
        // 5. 删除文件或文件夹
        fs.delete(new Path("/api_test2"), true);
    }

    //遍历hdfs的根目录得到文件以及文件夹的信息：名称，权限，长度等
    @Test
    public void listFiles() throws IOException {
        //得到一个迭代器，装有指定目录下所有文件信息
        RemoteIterator<LocatedFileStatus> remoteIterator = fs.listFiles(new Path("/"), true);
        //遍历迭代器
        while(remoteIterator.hasNext()){
            LocatedFileStatus fileStatus = remoteIterator.next();
            //文件名称
            String fileName = fileStatus.getPath().getName();
            //长度
            long len = fileStatus.getLen();
            //权限
            FsPermission permission = fileStatus.getPermission();
            //分组
            String group = fileStatus.getGroup();
            //用户
            String owner = fileStatus.getOwner();

            System.out.println(fileName + "\t" + len + "\t" + permission + "\t" + group + "\t" + owner);
            //块信息
            BlockLocation[] blockLocations = fileStatus.getBlockLocations();
            for (BlockLocation blockLocation : blockLocations) {
                String[] hosts = blockLocation.getHosts();
                for (String host : hosts) {
                    System.out.println("主机名称"+host);
                }
            }
        }
    }

    //文件以及文件夹判断
    //只是一层判断，没有递归判断的重载方法，需自己编程
    @Test
    public void isFile() throws IOException {
        FileStatus[] fileStatuses = fs.listStatus(new Path("/"));
        for (FileStatus fileStatus: fileStatuses) {
            boolean flag = fileStatus.isFile();
            if(flag){
                System.out.println("文件："+fileStatus.getPath().getName());
            } else {
                System.out.println("文件夹："+fileStatus.getPath().getName());
            }
        }
    }

    //使用IO流操作HDFS
    //上传文件：准备输入流读取本地文件，使用hdfs的输出流写出数据到hdfs
    @Test
    public void uploadFile() throws IOException {
        //1.读取本地文件的输入流
        FileInputStream inputStream = new FileInputStream(new File("E:\\code\\lagou.txt"));
        //2.准备写数据到hdfs的输出流
        FSDataOutputStream outputStream = fs.create(new Path("/lagou.txt"));
        //3.输入流数据拷贝到输出流：数组的大小，是否关闭流底层有默认值
        IOUtils.copyBytes(inputStream, outputStream, configuration);
        //4.可以再次关闭流
        IOUtils.closeStream(outputStream);
        IOUtils.closeStream(inputStream);
    }

    //使用IO流操作下载文件
    //使用IO流操作HDFS
    //上传文件：准备输入流读取本地文件，使用hdfs的输出流写出数据到hdfs
    @Test
    public void downloadFile() throws IOException {
        //1.读取hdfs文件的输入流
        FSDataInputStream inputStream1 = fs.open(new Path("/lagou.txt"));
        //2.准备写数据到本地的输出流
        FileOutputStream outputStream1 = new FileOutputStream(new File("E:\\code\\lagou.txt"));
        //3.输入流数据拷贝到输出流：数组的大小，是否关闭流底层有默认值
        IOUtils.copyBytes(inputStream1, outputStream1, configuration);
        //4.可以再次关闭流
        IOUtils.closeStream(outputStream1);
        IOUtils.closeStream(inputStream1);
    }

    //seek定位读取hdfs指定文件
    @Test
    public void seekReadFile() throws IOException {
        //1.创建一个读取hdfs文件的输入流
        FSDataInputStream in = fs.open(new Path("/lagou.txt"));
        //2.控制台数据：System.out
        //3.实现流copy，输入流-->控制台输出
        //IOUtils.copyBytes(in, System.out, configuration);   //这种方法读完后会关闭流
        IOUtils.copyBytes(in, System.out, 4096, false); //读完后不关闭流
        //4.再次读取文件
        in.seek(0);     //定位从0偏移量（文件头部）再次读取
        IOUtils.copyBytes(in, System.out, 4096, false); //读完后不关闭流
        //5.关闭流
        IOUtils.closeStream(in);

//        FSDataInputStream in = null;
//        try{
//            in= fs.open(new Path("/lagou.txt"));
//            IOUtils.copyBytes(in, System.out, 4096, false);
//            in.seek(0); //从头再次读取
//            IOUtils.copyBytes(in, System.out, 4096, false);
//        }finally {
//            IOUtils.closeStream(in);
//        }

    }

}
