package com.keli.platform.compact.bean;

import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;

public class SensorAlign {
    private String dev_id;
    private String timestamp;
    private String axis;
    private ArrayList<SensorThreshold> sensorThreshold;

    public SensorAlign(){};

    public SensorAlign(String dev_id, String timestamp, String axis, ArrayList<SensorThreshold> sensorThreshold) {
        this.dev_id = dev_id;
        this.timestamp = timestamp;
        this.axis = axis;
        this.sensorThreshold = sensorThreshold;
    }

    public String getDev_id() {
        return dev_id;
    }

    public void setDev_id(String dev_id) {
        if (dev_id == null || StringUtils.isBlank(dev_id)) {
            throw new IllegalArgumentException("invalid dev_id");
        }
        this.dev_id = dev_id.trim(); // 去掉首尾空格
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public String getAxis() {
        return axis;
    }

    public void setAxis(String axis) {
        this.axis = axis;
    }

    public ArrayList<SensorThreshold> getSensorThreshold() {
        return sensorThreshold;
    }

    public void setSensorThreshold(ArrayList<SensorThreshold> sensorThreshold) {
        this.sensorThreshold = sensorThreshold;
    }

    @Override
    public String toString() {
        return "SensorAlign{" +
                "dev_id='" + dev_id + '\'' +
                ", timestamp='" + timestamp + '\'' +
                ", axis='" + axis + '\'' +
                ", sensorThreshold=" + sensorThreshold +
                '}';
    }
}
