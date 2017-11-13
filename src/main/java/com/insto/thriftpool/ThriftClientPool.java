package com.insto.thriftpool;

import com.insto.thriftpool.exception.ConnectionFailException;
import com.insto.thriftpool.exception.NoBackendServiceException;
import com.insto.thriftpool.exception.ThriftException;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.thrift.TServiceClient;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Proxy;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;

public class ThriftClientPool<T extends TServiceClient> {

    private Logger logger = LoggerFactory.getLogger(getClass());
    private final Function<TTransport, T> clientFactory;
    private final GenericObjectPool<ThriftClient<T>> pool;
    private List<ServerInfo> services;
    private boolean serviceReset = false;
    private final PoolConfig poolConfig;

    public ThriftClientPool(List<ServerInfo> services, Function<TTransport, T> factory) throws Exception {
        this(services, factory, new PoolConfig(), null);
    }

    public ThriftClientPool(List<ServerInfo> services, Function<TTransport, T> factory,
            PoolConfig config) throws Exception {
        this(services, factory, config, null);
    }

    public ThriftClientPool(List<ServerInfo> services, Function<TTransport, T> factory,
            PoolConfig config, ThriftProtocolFactory pFactory) throws Exception {
        if (services == null || services.size() == 0) {
            throw new IllegalArgumentException("services is empty!");
        }
        if (factory == null) {
            throw new IllegalArgumentException("factory is empty!");
        }
        if (config == null) {
            throw new IllegalArgumentException("config is empty!");
        }

        this.services = services;
        this.clientFactory = factory;
        this.poolConfig = config;
        // test if config change
        this.poolConfig.setTestOnReturn(true);
        this.poolConfig.setTestOnBorrow(true);
        this.pool = new GenericObjectPool<>(new BasePooledObjectFactory<ThriftClient<T>>() {

            @Override
            public ThriftClient<T> create() throws Exception {
                // get from global list first
                List<ServerInfo> serviceList = ThriftClientPool.this.services;
                ServerInfo serverInfo = getRandomService(serviceList);
                TTransport transport = getTransport(serverInfo);

                try {
                    transport.open();
                } catch (TTransportException e) {
                    logger.info("transport open fail service: host={}, port={}", serverInfo.getHost(), serverInfo.getPort());
                    if (poolConfig.isFailover()) {
                        while (true) {
                            try {
                                // mark current fail and try next, until none service available
                                serviceList = removeFailService(serviceList, serverInfo);
                                serverInfo = getRandomService(serviceList);
                                transport = getTransport(serverInfo); // while break here
                                logger.info("failover to next service host={}, port={}", serverInfo.getHost(), serverInfo.getPort());
                                transport.open();
                                break;
                            } catch (TTransportException e2) {
                                logger.warn("failover fail, services left: {}", serviceList.size());
                            } finally {
                                TimeUnit.SECONDS.sleep(1);
                            }
                        }
                    } else {
                        throw new ConnectionFailException("host=" + serverInfo.getHost() + ", ip=" + serverInfo.getPort(), e);
                    }
                }

                ThriftClient<T> client = new ThriftClient<>(clientFactory.apply(transport), pool, serverInfo);

                logger.debug("create new object for pool {}", client);
                return client;
            }

            @Override
            public PooledObject<ThriftClient<T>> wrap(ThriftClient<T> obj) {
                return new DefaultPooledObject<>(obj);
            }

            @Override
            public boolean validateObject(PooledObject<ThriftClient<T>> p) {
                ThriftClient<T> client = p.getObject();

                // check if return client in current service list if 
                if (serviceReset) {
                    if (!ThriftClientPool.this.services.contains(client.getServiceInfo())) {
                        client.closeClient();
                        return false;
                    }
                }
                return super.validateObject(p);
            }

            @Override
            public void destroyObject(PooledObject<ThriftClient<T>> p) throws Exception {
                p.getObject().closeClient();
                super.destroyObject(p);
            }
        }, poolConfig);

        pool.preparePool();
    }

    public List<ServerInfo> getServices() {
        return services;
    }

    public void setServices(List<ServerInfo> services) {
        if (services == null || services.size() == 0) {
            throw new IllegalArgumentException("services is empty!");
        }
        this.services = services;
        serviceReset = true;
    }

    private TTransport getTransport(ServerInfo serverInfo) {
        if (serverInfo == null) {
            throw new NoBackendServiceException();
        }
        TTransport transport;
        if (poolConfig.getTimeout() > 0) {
            transport = new TSocket(serverInfo.getHost(), serverInfo.getPort(), poolConfig.getTimeout());
        } else {
            transport = new TSocket(serverInfo.getHost(), serverInfo.getPort());
        }
        return transport;
    }

    private ServerInfo getRandomService(List<ServerInfo> serviceList) {
        if (serviceList == null || serviceList.size() == 0) {
            return null;
        }
        return serviceList.get(RandomUtils.nextInt(0, serviceList.size()));
    }

    private List<ServerInfo> removeFailService(List<ServerInfo> list, ServerInfo serverInfo) {
        logger.info("remove service from current service list: host={}, port={}", serverInfo.getHost(), serverInfo.getPort());
        return list.stream()
                .filter(si -> !serverInfo.equals(si))
                .collect(Collectors.toList());
    }

    public ThriftClient<T> getClient() throws ThriftException {
        try {
            return pool.borrowObject();
        } catch (Exception e) {
            if (e instanceof ThriftException) {
                throw (ThriftException) e;
            }
            throw new ThriftException("Get client from pool failed.", e);
        }
    }

    public <X> X iface() throws ThriftException {
        ThriftClient<T> client;
        try {
            client = pool.borrowObject();
        } catch (Exception e) {
            if (e instanceof ThriftException) {
                throw (ThriftException) e;
            }
            throw new ThriftException("Get client from pool failed.", e);
        }
        AtomicBoolean returnToPool = new AtomicBoolean(false);
        return (X) Proxy.newProxyInstance(this.getClass().getClassLoader(), client.iFace()
                .getClass().getInterfaces(), (proxy, method, args) -> {
            if (returnToPool.get()) {
                throw new IllegalStateException("Object returned via iface can only used once!");
            }
            boolean success = false;
            try {
                Object result = method.invoke(client.iFace(), args);
                success = true;
                return result;
            } catch (Throwable e) {
                logger.warn("invoke fail", e);
                throw e;
            } finally {
                if (success) {
                    pool.returnObject(client);
                } else {
                    client.closeClient();
                    pool.invalidateObject(client);
                }
                returnToPool.set(true);
            }
        });
    }

    @Override
    protected void finalize() throws Throwable {
        if (pool != null) {
            pool.close();
        }
        super.finalize();
    }
}
