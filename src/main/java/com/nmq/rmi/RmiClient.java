package com.nmq.rmi;

import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;

/**
 * Created by niemengquan on 2017/3/10.
 */
public class RmiClient {
    public static void main(String[] args) throws RemoteException, NotBoundException, MalformedURLException {
        String url="rmi://localhost:1099/com.nmq.rmi.HelloServiceImpl";
        HelloService helloService = (HelloService) Naming.lookup(url);
        String result = helloService.sayHello("niemq");
        System.out.println(result);
    }
}
