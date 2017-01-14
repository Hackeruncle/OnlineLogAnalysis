package com.learn.java.main;


import java.io.Serializable;
/**
 * Created by jpwu on 2017/1/14.
 *
 * 基类
 *
 */
public class CDHRoleLog implements Serializable {
    private String hostName;
    private String serviceName;
    private String lineTimestamp;
    private String logType;
    private String logInfo;


    public CDHRoleLog( String hostName, String serviceName, String lineTimestamp, String logType, String logInfo){
        this.hostName=hostName;
        this.serviceName=serviceName;
        this.lineTimestamp=lineTimestamp;
        this.logType=logType;
        this.logInfo=logInfo;
    }

    public String getHostName() {
        return hostName;
    }

    public void setHostName(String hostName) {
        this.hostName = hostName;
    }

    public String getServiceName() {
        return serviceName;
    }

    public void setServiceName(String serviceName) {
        this.serviceName = serviceName;
    }

    public String getLineTimestamp() {
        return lineTimestamp;
    }

    public void setLineTimestamp(String lineTimestamp) {
        this.lineTimestamp = lineTimestamp;
    }

    public String getLogType() {
        return logType;
    }

    public void setLogType(String logType) {
        this.logType = logType;
    }

    public String getLogInfo() {
        return logInfo;
    }

    public void setLogInfo(String logInfo) {
        this.logInfo = logInfo;
    }


    @Override public String toString() {

        return String.format("%s %s %s %s %s",
                hostName, serviceName, lineTimestamp, logType,logInfo);
    }

}
