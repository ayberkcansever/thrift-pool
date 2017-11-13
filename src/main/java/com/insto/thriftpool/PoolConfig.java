package com.insto.thriftpool;

import lombok.Getter;
import lombok.Setter;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

public class PoolConfig extends GenericObjectPoolConfig {

    @Getter @Setter private int timeout = 0;
    @Getter @Setter private boolean failover = false;

}
