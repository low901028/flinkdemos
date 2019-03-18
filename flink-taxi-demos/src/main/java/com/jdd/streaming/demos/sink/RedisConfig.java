package com.jdd.streaming.demos.sink;

/**
 * @Auther: dalan
 * @Date: 19-3-15 14:39
 * @Description:
 */
import java.io.Serializable;

public class RedisConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    private String host = "127.0.0.1";
    private int port = 6379;
    private int database = 0;
    private String password = null;
    protected int maxTotal = 8;
    protected int maxIdle = 8;
    protected int minIdle = 0;
    protected int connectionTimeout = 2000;

    public RedisConfig host(String host) {
        this.host = (host);
        return this;
    }

    public RedisConfig port(int port) {
        this.port = (port);
        return this;
    }

    public RedisConfig database(int database) {
        this.database = (database);
        return this;
    }

    public RedisConfig password(String password) {
        this.password = (password);
        return this;
    }

    public RedisConfig maxTotal(int maxTotal) {
        this.maxTotal = (maxTotal);
        return this;
    }

    public RedisConfig maxIdle(int maxIdle) {
        this.maxIdle = (maxIdle);
        return this;
    }

    public RedisConfig minIdle(int minIdle) {
        this.minIdle = (minIdle);
        return this;
    }

    public RedisConfig connectionTimeout(int connectionTimeout) {
        this.connectionTimeout = (connectionTimeout);
        return this;
    }

    public static long getSerialVersionUID() {
        return serialVersionUID;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public int getDatabase() {
        return database;
    }

    public void setDatabase(int database) {
        this.database = database;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public int getMaxTotal() {
        return maxTotal;
    }

    public void setMaxTotal(int maxTotal) {
        this.maxTotal = maxTotal;
    }

    public int getMaxIdle() {
        return maxIdle;
    }

    public void setMaxIdle(int maxIdle) {
        this.maxIdle = maxIdle;
    }

    public int getMinIdle() {
        return minIdle;
    }

    public void setMinIdle(int minIdle) {
        this.minIdle = minIdle;
    }

    public int getConnectionTimeout() {
        return connectionTimeout;
    }

    public void setConnectionTimeout(int connectionTimeout) {
        this.connectionTimeout = connectionTimeout;
    }
}
