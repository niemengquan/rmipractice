package com.nmq.zookeeper;

import org.apache.zookeeper.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.util.concurrent.CountDownLatch;

/**
 * Created by niemengquan on 2017/3/10.
 */
public class ServiceProvider {
    private static final Logger LOGGER= LoggerFactory.getLogger(ServiceProvider.class);

    //用于等待SyncConnected 事件触发后继续执行当前线程
    private CountDownLatch latch=new CountDownLatch(1);

    /**
     * 连接ZooKeeper 服务器
     * @return
     */
    private ZooKeeper connectServer(){
        ZooKeeper zk=null;
        try{
            zk=new ZooKeeper(Constant.ZK_CONNECTION_STRING, Constant.ZK_SESSION_TIMEOUT, new Watcher() {
                public void process(WatchedEvent event) {
                    if(event.getState()==Event.KeeperState.SyncConnected){
                        latch.countDown();//唤醒当前正在执行的线程
                    }
                }
            });
            latch.await();//使当前线程处于等待状态
        }catch (Exception err){
            err.printStackTrace();
        }
        return zk;
    }

    /**
     * 创建ZNode
     *
     * @param zk
     * @param url
     */
    private void createNode(ZooKeeper zk,String url){
        byte[] data = url.getBytes();
        try {
            String path = zk.create(Constant.ZK_PROVIDER_PATH, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
            LOGGER.debug("create zookeeper node ({} =>{})",path,url);

        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * 发布RMId 服务
     * @param remote
     * @param host
     * @param port
     * @return
     */
    private String publishService(Remote remote,String host,int port){
        String url=null;
        try{
            url = String.format("rmi://%s:%d/%s", host, port, remote.getClass().getName());
            LocateRegistry.createRegistry(port);
            Naming.rebind(url,remote);
            LOGGER.debug("publish rmi service (url: {})", url);
        }catch (RemoteException err){
            LOGGER.error("",err);
        }catch (MalformedURLException err){
            LOGGER.error("",err);
        }
        return url;
    }

    /**
     * 发布 RMI 服务并注册 RMI 地址到 ZooKeeper 中
     * @param remote
     * @param host
     * @param port
     */
    public void publish(Remote remote,String host,int port){
        String url=publishService(remote,host,port);//发布 RMI 服务并返回 RMI 地址
        if(url!=null){
            ZooKeeper zk=connectServer();//连接 ZooKeeper 服务器并获取 ZooKeeper 对象
            if(zk!=null){
                createNode(zk,url);//创建 ZNode 并将 RMI 地址放入 ZNode 上
            }
        }
    }
}
