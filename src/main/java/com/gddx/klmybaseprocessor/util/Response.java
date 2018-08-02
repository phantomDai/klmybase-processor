package com.gddx.klmybaseprocessor.util;

import com.alibaba.fastjson.JSONObject;

import java.util.HashMap;
import java.util.Map;

public class Response {

    public static JSONObject successResponse(Map<String,String> filesAndRowkeys){
        Map<String,String> mapheader = new HashMap<>();
        mapheader.put("code",String.valueOf(0));
        mapheader.put("message","");
        Map<String,Object> responseMap = new HashMap<>();
        responseMap.putAll(mapheader);
        Map<String,Map<String,String>> extraFilesAndRowkeys = new HashMap<>();
        extraFilesAndRowkeys.put("data",filesAndRowkeys);
        responseMap.putAll(extraFilesAndRowkeys);
        JSONObject responseJson = new JSONObject(responseMap);
        return responseJson;
    }

    public static JSONObject failedResponse(String message){
        Map<String,Object> responseMap = new HashMap<>();
        responseMap.put("code",String.valueOf(1));
        responseMap.put("message",message);
        JSONObject response = new JSONObject(responseMap);
        return response;
    }

    public static JSONObject successResponse(){
        Map<String,Object> map = new HashMap<>();
        map.put("code",String.valueOf(0));
        map.put("message","");
        JSONObject responseJson = new JSONObject(map);
        return responseJson;
    }

    public static JSONObject FileResponse(String code, String message, Map<String,Object> dataMap){
        Map<String,Object> responseMap = new HashMap<>();
        responseMap.put("cade", code);
        responseMap.put("message", message);
        responseMap.put("data",dataMap);
        return new JSONObject(responseMap);
    }












}
