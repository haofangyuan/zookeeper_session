package com.hfy.zksession;

import java.io.Serializable;
import java.util.Map;

public class SessionMetaData implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * sessionId
     */
    private String id;

    /**
     * session创建时间
     */
    private Long createdTime;

    /**
     * 最后一次访问时间
     */
    private Long lastAccessTime;
    /**
     * session的最大空闲时间，默认30分钟
     */
    private Long maxIdle = 30 * 60 * 1000L;
    /**
     * 当前版本 这个属性是为了冗余Znode的version值，用来实现乐观锁，对Session节点的元数据进行更新操作。
     */
    private int version = 0;

    /**
     * session内容体
     */
    private Map<String, String> sessionContext;

    public SessionMetaData() {
        this.createdTime = System.currentTimeMillis();
        this.lastAccessTime = this.createdTime;
    }

    public SessionMetaData(String id) {
        this.id = id;
        this.createdTime = System.currentTimeMillis();
        this.lastAccessTime = this.createdTime;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Long getCreatedTime() {
        return createdTime;
    }

    public void setCreatedTime(Long createdTime) {
        this.createdTime = createdTime;
    }

    public Long getLastAccessTime() {
        return lastAccessTime;
    }

    public void setLastAccessTime(Long lastAccessTime) {
        this.lastAccessTime = lastAccessTime;
    }

    public Map<String, String> getSessionContext() {
        return sessionContext;
    }

    public void setSessionContext(Map<String, String> sessionContext) {
        this.sessionContext = sessionContext;
    }

    public Long getMaxIdle() {
        return maxIdle;
    }

    public void setMaxIdle(Long maxIdle) {
        this.maxIdle = maxIdle;
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }
}