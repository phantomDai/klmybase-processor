package com.gddx.klmybaseprocessor.controller;

import com.alibaba.fastjson.JSONObject;
import com.gddx.klmybaseprocessor.util.ChangeFileName;
import com.gddx.klmybaseprocessor.util.Response;
import com.gddx.klmybaseprocessor.util.TransformFileToBytes;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.multipart.MultipartHttpServletRequest;


import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;


@RestController
public class FileProcessor {

    //the object is used to configurate Hbase
    private Configuration configurationHbase;

    //this object is a Communicator to hbase
    private Connection connectionHbase;

    //the name of table that save information of files
    private static final String TABLENAME = "file_table" ;

    private static final String FILEPATH = "/home/dev/";


    //the object is used to configurate Hadoop
    private Configuration configurationHadoop;

    //this object is a communicator to hadoop
    private FileSystem fileSystem;

    /**
     * initialization hbase before operating files
     */
    private void initHbase(){
        // initialize configuration and connection
        configurationHbase = HBaseConfiguration.create();
        configurationHbase.set(HConstants.ZOOKEEPER_QUORUM, "10.0.0.52");
        connectionHbase = null;
        try {
            connectionHbase = ConnectionFactory.createConnection(configurationHbase);
        } catch (IOException e) {
            System.out.println("Hbase 初始化错误");
            e.printStackTrace();
        }
    }

    /**
     * initialization hadoop before operating files
     */
    private void initHadoop(){
        configurationHadoop = new Configuration();
        configurationHadoop.set("fs.hdfs.impl",org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        try {
            fileSystem = FileSystem.get(new URI("hdfs://10.0.0.51:8020"),configurationHadoop,"hdfs");
        } catch (IOException e) {
            System.out.println("初始化hadoop错误");
        } catch (InterruptedException e) {
            System.out.println("初始化hadoop错误");
            e.printStackTrace();
        } catch (URISyntaxException e) {
            System.out.println("初始化hadoop错误");
            e.printStackTrace();
        }

    }


    /**
     * @param filename the name of file
     * this method is used to generate a rowkey, which
     * follow the principle: suffix + (@_@) + timestamps + (@_@) +random number
     * @return rowkey
     */
    private String generateRowkey(String filename){
        String rowkey = "";
        //get current time
        String currentTime = String.valueOf(System.currentTimeMillis());
        // get a random number
        Random ran = new Random();
        int tempRandom = ran.nextInt(10000);
        // make sure that all numbers is bigger than 1000
        if (tempRandom < 1000)
            tempRandom = tempRandom + 1000;
        //get suffix
        String suffix = filename.substring(filename.lastIndexOf(".") + 1);

        // generate a rowkey
        rowkey = suffix + "(@_@)" + currentTime + "(@_@)" +String.valueOf(tempRandom);
        return rowkey;
    }

    @PostMapping(value = "/api/file/upload/smallerfiles/")
    @ResponseBody
    public JSONObject uploadSmallerFiles(HttpServletRequest request) {
        List<MultipartFile> files = ((MultipartHttpServletRequest) request).getFiles("file");

        MultipartFile file = null;


        //get connection Hbase
        initHbase();

        // this map is used to save the names of files and consistent row keys
        List<Map<String,Object>> mapInfo = new ArrayList<>();

        //record the number of failed file
        int failedFile = 0;

        if (files != null && files.size() > 0){
            for (int i = 0; i < files.size(); i++) {
                Map<String,Object> tempMap = new HashMap<>();
                tempMap.clear();
                String fileName = "";
                file = files.get(i);
                fileName = file.getOriginalFilename();
                // get a row key
                String tempRowKey = generateRowkey(fileName);
                try {
                    byte[] fileBytes = file.getBytes();
                    Put put = new Put(Bytes.toBytes(tempRowKey));
                    put.addColumn(Bytes.toBytes("fileInfo"),Bytes.toBytes("file_name"),Bytes.toBytes(fileName));
                    put.addColumn(Bytes.toBytes("fileInfo"),Bytes.toBytes("file_content"),fileBytes);
                    Table table = connectionHbase.getTable(TableName.valueOf(TABLENAME));
                    table.put(put);
                    tempMap.put("id",tempRowKey);
                    tempMap.put("code", String.valueOf(0));
                    tempMap.put("message",fileName);
                } catch (IOException e) {
                    failedFile++;
                    tempMap.put("id",tempRowKey);
                    tempMap.put("code", String.valueOf(-1));
                    tempMap.put("message",fileName);
                    e.printStackTrace();
                }
                mapInfo.add(tempMap);
            }
        }else {
            return Response.failedResponse("请检查是否上传了文件");
        }
        Map<String,Object> internalMap = new HashMap<>();
        internalMap.put("total",files.size());
        internalMap.put("error",String.valueOf(failedFile));
        internalMap.put("data",mapInfo);
        return Response.FileResponse(String.valueOf(6),"",internalMap);
    }


    @GetMapping(value = "/api/file/download/smallerfiles/{id}",produces = MediaType.MULTIPART_FORM_DATA_VALUE)
    public JSONObject downloadSmallerFiles(@PathVariable("id")String rowkey, HttpServletResponse res){


        //get connection Hbase
        initHbase();
        //get instance of table
        Table table = null;

        Map<String,Object> tempMap = new HashMap<>();
        tempMap.put("id",rowkey);

        try {
            table = connectionHbase.getTable(TableName.valueOf(TABLENAME));
        } catch (IOException e) {
            System.out.println("获取数据库表的实例时出错");
            e.printStackTrace();
        }
        //get files' bytes
        Get get = new Get(Bytes.toBytes(rowkey));
        Result result = null;
        try {
            result = table.get(get);
        } catch (IOException e) {
            System.out.println("找不到行键对应的文件");
        }
        //get file's name
        String name = Bytes.toString(result.getValue(Bytes.toBytes("fileInfo"),Bytes.toBytes("file_name")));
        tempMap.put("name",name);

        //set header
        res.setContentType("application/force-download");
        res.setHeader("Content-Disposition", "attachment;fileName=" + name);

        //get file's byte code
        byte[] content = result.getValue(Bytes.toBytes("fileInfo"),Bytes.toBytes("file_content"));
        try {
            ByteArrayInputStream inputStream = new ByteArrayInputStream(content);
            OutputStream outputStream = res.getOutputStream();
            byte[] buffer = new byte[1024];
            int count;
            while((count = inputStream.read(buffer)) != -1){
                outputStream.write(buffer,0,count);
            }
            outputStream.flush();
            outputStream.close();
            inputStream.close();

        } catch (IOException e) {
            e.printStackTrace();
        }


        Map<String,Object> internalMap = new HashMap<>();
        internalMap.put("data",tempMap);
        return Response.FileResponse("0","成功",internalMap);
    }

    @PostMapping(value = "/api/file/upload/biggerfiles/")
    @ResponseBody
    public JSONObject uploadBiggerFiles(HttpServletRequest request){

        List<MultipartFile> files = ((MultipartHttpServletRequest) request).getFiles("file");

        initHadoop();
        //record the number of failed file
        int failedFile = 0;

        //get the number of files in the hadoop system
        FileStatus[] fileStatus = null;
        try {
            fileStatus = fileSystem.listStatus(new Path(FILEPATH));
        } catch (IOException e) {
            e.printStackTrace();
        }
        int cruNumberOfFiles = fileStatus.length;

        ChangeFileName changeFileName = new ChangeFileName();

        List<Map<String,Object>> listInfo = new ArrayList<>();


        for (int i = 0; i < files.size(); i++) {
            Map<String,Object> tempMap = new HashMap<>();

            MultipartFile file = files.get(i);
            cruNumberOfFiles++;
            changeFileName.changeFileName(file.getOriginalFilename(),String.valueOf(cruNumberOfFiles));

            //turn a file into bytes
            byte[] content = new byte[]{};

            //The path stored in hadoop
            Path dst = new Path("/home/dev/" + changeFileName.getFinalName());

            //open a output stream
            try {
                InputStream in = file.getInputStream();
                content = null;
                content = TransformFileToBytes.inputStreamToByte(in);
                FSDataOutputStream out = fileSystem.create(dst);
                out.write(content);
                out.close();
                in.close();
                tempMap.put("id", String.valueOf(cruNumberOfFiles));
                tempMap.put("code",String.valueOf(0));
                tempMap.put("message","操作成功,文件名为：" + file.getOriginalFilename());
                listInfo.add(tempMap);
            } catch (IOException e) {
                failedFile++;
                tempMap.put("id", String.valueOf(cruNumberOfFiles));
                tempMap.put("code",String.valueOf(-1));
                tempMap.put("message","操作失败,文件名为：" + file.getOriginalFilename());
                listInfo.add(tempMap);
            }
        }

        try {
            fileSystem.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        Map<String,Object> internalMap = new HashMap<>();
        internalMap.put("total",String.valueOf(files.size()));
        internalMap.put("error",String.valueOf(failedFile));
        internalMap.put("data",listInfo);
        return Response.FileResponse(String.valueOf(6),"",internalMap);
    }

    @GetMapping(value = "/api/file/download/biggerfiles/{id}", produces = MediaType.MULTIPART_FORM_DATA_VALUE)
    public JSONObject downloadBiggerFiles(@PathVariable("id")String id,HttpServletResponse res){

        initHadoop();
        String fileName = "";
        try {
            FileStatus[] fileStatuses = fileSystem.listStatus(new Path("/home/dev/"));
            if (fileStatuses == null){
                System.out.println("无文件列表++++++++++++++++++++++++++++++");
            }else {
                System.out.println("获得了所有的文件状态");
            }

            for (FileStatus fs : fileStatuses) {
                if (fs.getPath().getName().contains("_"+ id +".")){
                    fileName = fs.getPath().getName();
                    break;
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

        //set http response header
        res.setContentType("application/force-download");
        res.setHeader("Content-Disposition", "attachment;fileName=" + fileName);

        Path drc = new Path("/home/dev/" + fileName);
        try {
            FSDataInputStream inputStream = fileSystem.open(drc);
            OutputStream outputStream = res.getOutputStream();
            byte[] buffer = new byte[1024];
            int count;
            while((count = inputStream.read(buffer)) != -1){
                outputStream.write(buffer,0,count);
            }
            outputStream.flush();
            outputStream.close();
            inputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        Map<String,Object> tempMap = new HashMap<>();
        tempMap.put("id",id);
        Map<String,Object> internalMap = new HashMap<>();
        internalMap.put("data",tempMap);
        return Response.FileResponse("0","成功",internalMap);
    }

}
