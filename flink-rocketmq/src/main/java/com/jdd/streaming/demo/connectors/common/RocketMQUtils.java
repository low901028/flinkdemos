package com.jdd.streaming.demo.connectors.common;

import java.io.Serializable;
import java.util.Properties;

/**
 * @Auther: dalan
 * @Date: 19-4-2 19:00
 * @Description:
 */
public class RocketMQUtils {
    public static int getInteger(Properties props, String key, int defaultValue) {
        return Integer.parseInt(props.getProperty(key, String.valueOf(defaultValue)));
    }

    public static long getLong(Properties props, String key, long defaultValue) {
        return Long.parseLong(props.getProperty(key, String.valueOf(defaultValue)));
    }

    public static boolean getBoolean(Properties props, String key, boolean defaultValue) {
        return Boolean.parseBoolean(props.getProperty(key, String.valueOf(defaultValue)));
    }

    // rocketmq running check
    public static class RunningChecker implements Serializable {
        private volatile boolean isRunning = false;

        public boolean isRunning() {
            return isRunning;
        }

        public void setRunning(boolean running) {
            isRunning = running;
        }
    }
}


