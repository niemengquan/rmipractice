package com.nmq.zookeeper;

import com.nmq.rmi.HelloService;

/**
 * Created by niemengquan on 2017/3/10.
 */
public class Client {
    public static void main(String[] args) throws Exception{
        ServiceConsumer consumer=new ServiceConsumer();

        while (true){
            HelloService helloService=consumer.lookup();
            String result = helloService.sayHello("niemq");
            System.out.println(result);
            Thread.sleep(3000);
        }

    }
}
