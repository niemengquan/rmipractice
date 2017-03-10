package com.nmq.zookeeper;

import org.apache.zookeeper.*;

/**
 * Created by niemengquan on 2017/3/10.
 */
public class ConnZookeeperTest {
    public static void main(String[] args) throws Exception{
        ZooKeeper  zk = new ZooKeeper("localhost:2181", 5000, new Watcher() {
            //监控所有触发的事件
            public void process(WatchedEvent watchedEvent) {
                System.out.println("已经触发了"+watchedEvent.getType()+"事件！+"+watchedEvent.getState()+watchedEvent.getPath());
            }
        });
        //创建一个目录节点
        zk.create("/testRootPath","testRootPath".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        // 创建一个子目录节点
        zk.create("/testRootPath/testChildPathOne", "testChildDataOne".getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
        System.out.println(new String(zk.getData("/testRootPath",true,null)));
        // 取出子目录节点列表
        System.out.println(zk.getChildren("/testRootPath",true));
        // 修改子目录节点数据
        zk.setData("/testRootPath/testChildPathOne","modifyChildDataOne".getBytes(),-1);
        System.out.println("目录节点状态：["+zk.exists("/testRootPath",true)+"]");
        // 创建另外一个子目录节点
        zk.create("/testRootPath/testChildPathTwo", "testChildDataTwo".getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
        System.out.println(new String(zk.getData("/testRootPath/testChildPathTwo",true,null)));
        // 删除子目录节点
        zk.delete("/testRootPath/testChildPathTwo",-1);
        zk.delete("/testRootPath/testChildPathOne",-1);
        // 删除父目录节点
        zk.delete("/testRootPath",-1);
        //关闭连接
        zk.close();
    }
}
