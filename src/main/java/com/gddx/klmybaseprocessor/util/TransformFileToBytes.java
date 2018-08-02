package com.gddx.klmybaseprocessor.util;


import java.io.*;
import java.util.HashMap;
import java.util.Map;


/**
 * transform files to bytes
 */
public class TransformFileToBytes {

    public static Map<String,byte[]> transformFileToBytes(Map<String,String> filesInfo) {
        //put the name and bytes of a file into MapInfo
        Map<String,byte[]> mapInfo = new HashMap<>();
        //traverse map
        for (Map.Entry<String,String> entry : filesInfo.entrySet()) {
            //get a file
            File tempfile = new File(entry.getValue());

            byte[] fileByte = new byte[]{};
            try {
                InputStream in = new FileInputStream(tempfile);
                fileByte = inputStreamToByte(in);
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
            mapInfo.put(entry.getKey(),fileByte);
        }
        return mapInfo;
    }

    public static byte [] inputStreamToByte(InputStream is) throws IOException {
        ByteArrayOutputStream bAOutputStream = new ByteArrayOutputStream();
        int ch;
        while((ch = is.read() ) != -1){
            bAOutputStream.write(ch);
        }
        byte data [] =bAOutputStream.toByteArray();
        bAOutputStream.close();
        return data;
    }
}
