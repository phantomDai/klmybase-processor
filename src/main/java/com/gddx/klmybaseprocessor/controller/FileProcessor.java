package com.gddx.klmybaseprocessor.controller;

import com.alibaba.fastjson.JSONObject;
import com.gddx.klmybaseprocessor.util.ChangeFileName;
import com.gddx.klmybaseprocessor.util.Response;
import com.gddx.klmybaseprocessor.util.TransformFileToBytes;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;


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
        configurationHbase.set("hbase.zookeeper.quorum", "10.0.0.51");
        try {
            connectionHbase = ConnectionFactory.createConnection(configurationHbase);
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }
    }

    /**
     * initialization hadoop before operating files
     */
    private void initHadoop(){
        configurationHbase = new Configuration();
        configurationHadoop.set("fs.hdfs.impl",org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        try {
            fileSystem = FileSystem.get(new URI("hdfs://10.0.0.52:8020"),configurationHadoop,"hdfs");

        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }


    }


    /**
     * this method is used to generate a rowkey, which
     * follow the principle: a random number + timestamps
     * @return rowkey
     */
    private String generateRowkey(){
        String rowkey = "";
        //get current time
        String currentTime = String.valueOf(System.currentTimeMillis());
        // get a random number
        Random ran = new Random();
        int tempRandom = ran.nextInt(10000);
        // make sure that all numbers is bigger than 1000
        if (tempRandom < 1000)
            tempRandom = tempRandom + 1000;

        // generate a rowkey
        rowkey = String.valueOf(tempRandom) + currentTime;
        return rowkey;
    }




    @PostMapping(value = "/upload/smallerfiles/")
    public JSONObject uploadSmallerFiles(@RequestParam("files") MultipartFile[] files) {
        //get connection Hbase
        initHbase();

        // this map is used to save the names of files and consistent row keys
        List<Map<String,Object>> mapInfo = new ArrayList<>();

        //record the number of failed file
        int failedFile = 0;


        if (files != null && files.length > 0){
            for (int i = 0; i < files.length; i++) {
                Map<String,Object> tempMap = new HashMap<>();
                tempMap.clear();
                String fileName = "";
                fileName = files[i].getOriginalFilename();
                // get a row key
                String tempRowKey = generateRowkey();
                try {
                    byte[] fileBytes = files[i].getBytes();
                    Put put = new Put(Bytes.toBytes(tempRowKey));
                    put.addColumn(Bytes.toBytes("fileInfo"),Bytes.toBytes("file_name"),Bytes.toBytes(fileName));
                    put.addColumn(Bytes.toBytes("fileInfo"),Bytes.toBytes("file_content"),fileBytes);
                    Table table = connectionHbase.getTable(TableName.valueOf(TABLENAME));
                    table.put(put);
                    tempMap.put("id",tempRowKey);
                    tempMap.put("code", String.valueOf(0));
                    tempMap.put("message","");
                } catch (IOException e) {
                    failedFile++;
                    tempMap.put("id",tempRowKey);
                    tempMap.put("code", String.valueOf(-1));
                    tempMap.put("message","");
                    e.printStackTrace();
                }
                mapInfo.add(tempMap);
            }
        }else {
            return Response.failedResponse("请检查是否上传了文件");
        }
        Map<String,Object> internalMap = new HashMap<>();
        internalMap.put("total",files.length);
        internalMap.put("error",String.valueOf(failedFile));
        internalMap.put("data",mapInfo);
        return Response.FileResponse(String.valueOf(6),"",internalMap);
    }


    @GetMapping(value = "/download/smallerfiles/{id}")
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
            e.printStackTrace();
            return Response.failedResponse("获取数据库表的实例时出错");
        }
        //get files' bytes
        Get get = new Get(Bytes.toBytes(rowkey));
        Result result = null;
        try {
            result = table.get(get);
        } catch (IOException e) {
            return Response.failedResponse("找不到行键对应的文件");
        }
        //get file's name
        String name = Bytes.toString(result.getValue(Bytes.toBytes("fileInfo"),Bytes.toBytes("file_name")));
        tempMap.put("name",name);
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


    @PostMapping(value = "/upload/biggerfiles/")
    public JSONObject uploadBiggerFiles(@RequestParam("files") MultipartFile[] files){
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

        for (int i = 0; i < files.length; i++) {
            Map<String,Object> tempMap = new HashMap<>();

            cruNumberOfFiles++;
            changeFileName.changeFileName(files[i].getOriginalFilename(),String.valueOf(cruNumberOfFiles));

            //turn a file into bytes
            byte[] content = new byte[]{};

            //The path stored in hadoop
            Path dst = new Path("/home/dev/" + changeFileName.getFinalName());
            //open a output stream
            try {
                InputStream in = files[i].getInputStream();
                content = TransformFileToBytes.inputStreamToByte(in);
                FSDataOutputStream out = fileSystem.create(dst);
                out.write(content);
                out.close();
                in.close();
                tempMap.put("id", String.valueOf(cruNumberOfFiles));
                tempMap.put("code",String.valueOf(0));
                tempMap.put("message","操作成功");
                listInfo.add(tempMap);
            } catch (IOException e) {
                failedFile++;
                tempMap.put("id", String.valueOf(cruNumberOfFiles));
                tempMap.put("code",String.valueOf(-1));
                tempMap.put("message","操作失败");
            }
            try {
                fileSystem.close();
            } catch (IOException e) {
                e.printStackTrace();
            }

        }
        Map<String,Object> internalMap = new HashMap<>();
        internalMap.put("total",String.valueOf(fileStatus.length));
        internalMap.put("error",String.valueOf(failedFile));
        internalMap.put("data",listInfo);
        return Response.FileResponse(String.valueOf(6),"",internalMap);
    }

    @GetMapping(value = "/download/biggerfiles/{id}")
    public JSONObject downloadBiggerFiles(@PathVariable("id")String id,HttpServletResponse res){
        initHadoop();
        String fileName = "";
        try {
            FileStatus[] fileStatuses = fileSystem.listStatus(new Path(FILEPATH));
            for (FileStatus fs : fileStatuses) {
                if (fs.getPath().getName().contains("_"+ id +".")){
                    fileName = fs.getPath().getName();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        Path drc = new Path(FILEPATH + fileName);
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
