package com.insto.thriftpool;

import lombok.Getter;
import lombok.Setter;
import org.apache.commons.pool2.ObjectPool;
import org.apache.thrift.TServiceClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.lang.reflect.Method;
import java.util.concurrent.TimeUnit;

public class ThriftClient<T extends TServiceClient> implements Runnable, Closeable {

    private Logger logger = LoggerFactory.getLogger(getClass());
    @Getter @Setter private final T client;
    private final ObjectPool<ThriftClient<T>> pool;
    private boolean finish;
    @Getter private final ServerInfo serviceInfo;
    @Getter @Setter private boolean running = true;
    @Getter @Setter private long pingStartDelayInSec = 5;
    @Getter @Setter private long pingIntervalInSec = 10;
    @Getter @Setter private Class clientClass;
    @Getter @Setter private boolean pingEnabledClient = false;

    public ThriftClient(T client, ObjectPool<ThriftClient<T>> pool, ServerInfo serviceInfo) {
        this.client = client;
        this.pool = pool;
        this.serviceInfo = serviceInfo;
        clientClass = client.getClass();
        for(Method method : clientClass.getMethods()) {
            if(method.getName().equals("ping")) {
                pingEnabledClient = true;
                break;
            }
        }
        if(pingEnabledClient) {
            logger.info("Ping enabled thrift client created.");
            new Thread(this).start();
        }
    }

    public T iFace() {
        return client;
    }

    @Override
    public void close() {
        try {
            if (finish) {
                logger.debug("return object to pool: " + this);
                finish = false;
                pool.returnObject(this);
            } else {
                logger.debug("not return object cause not finish {}", client);
                closeClient();
                pool.invalidateObject(this);
            }
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("return object fail, close", e);
            closeClient();
        }
    }

    void closeClient() {
        logger.debug("close client {}", this);
        running = false;
        ThriftUtil.closeClient(this.client);
    }

    public void finish() {
        this.finish = true;
    }

    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        closeClient();
    }

    @Override
    public void run() {
        try {
            TimeUnit.SECONDS.sleep(pingStartDelayInSec);
        } catch (Exception e) {
            e.printStackTrace();
        }
        while (running) {
            try {
                for(Method method : clientClass.getMethods()) {
                    if (method.getName().equals("ping")) {
                        if(method.getReturnType() == String.class) {
                            String pong = (String) method.invoke(this.client);
                            logger.info(pong);
                        } else {
                            method.invoke(this.client);
                        }
                        break;
                    }
                }
            } catch (Exception ex) {
                ex.printStackTrace();
            } finally {
                try {
                    TimeUnit.SECONDS.sleep(pingIntervalInSec);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
