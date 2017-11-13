package com.insto.thriftpool;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

public class PoolConfig extends GenericObjectPoolConfig {

    private int timeout = 0;
    private boolean failover = false;

    public int getTimeout() {
        return timeout;
    }
    public void setTimeout(int timeout) {
        this.timeout = timeout;
    }
    public boolean isFailover() {
        return failover;
    }
    public void setFailover(boolean failover) {
        this.failover = failover;
    }

}
