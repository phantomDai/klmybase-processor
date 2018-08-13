package com.gddx.klmybaseprocessor.util;

public class ObtainFileType {

    public static String obtainFileType(String fileName){
        return fileName.substring(fileName.lastIndexOf(".") + 1);
    }



}
