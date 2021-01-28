package com.keli.platform.compact.bean;

import org.apache.commons.lang3.StringUtils;

public class SensorThreshold {
    private String sensor_id;
    private Double mean;
    private Double upper_threshold;
    private Double lower_threshold;

    public SensorThreshold(){};

    public SensorThreshold(String sensor_id, Double mean, Double upper_threshold, Double lower_thre) {
        this.sensor_id = sensor_id;
        this.mean = mean;
        this.upper_threshold = upper_threshold;
        this.lower_threshold = lower_thre;
    }

    public String getSensor_id() {
        return sensor_id;
    }

    public void setSensor_id(String sensor_id) {
        if (sensor_id == null || StringUtils.isBlank(sensor_id)) {
            throw new IllegalArgumentException("invalid sensor_id");
        }
        this.sensor_id = sensor_id.trim(); // 去掉首尾空格
    }

    public Double getMean() {
        return mean;
    }

    public void setMean(Double mean) {
        this.mean = mean;
    }

    public Double getUpper_threshold() {
        return upper_threshold;
    }

    public void setUpper_threshold(Double upper_threshold) {
        this.upper_threshold = upper_threshold;
    }

    public Double getLower_threshold() {
        return lower_threshold;
    }

    public void setLower_threshold(Double lower_threshold) {
        this.lower_threshold = lower_threshold;
    }

    @Override
    public String toString() {
        return "SensorThreshold{" +
                "sensor_id='" + sensor_id + '\'' +
                ", mean=" + mean +
                ", upper_thre=" + upper_threshold +
                ", lower_thre=" + lower_threshold +
                '}';
    }
}
