package com.gddx.klmybaseprocessor.util;

public class ChangeFileName {
    private String name;
    private String suffix;
    private String finalName;



    public void changeFileName(String fileName, String number){
        String tempName = null;
        tempName = fileName.substring(0,fileName.lastIndexOf(".")) + "_" + number;
        setName(tempName);
        setSuffix(fileName.substring(fileName.lastIndexOf(".")));
    }


    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getSuffix() {
        return suffix;
    }

    public void setSuffix(String suffix) {
        this.suffix = suffix;
    }

    public String getFinalName() {
        return getName() + getSuffix();
    }

}
