package com.example.rocketmq;

/**
 * @author ttchengwang@foxmail.com
 */
public class RocketConfig {
    private static String nameServer = "119.3.52.162:9876";

    public static synchronized String getNameServer() {
        return nameServer;
    }
}
