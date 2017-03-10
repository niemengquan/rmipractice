package com.nmq.rmi;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

/**
 * Created by niemengquan on 2017/3/10.
 */
public class HelloServiceImpl extends UnicastRemoteObject implements HelloService  {

    public HelloServiceImpl() throws RemoteException {
    }

    public String sayHello(String name) throws RemoteException {
        return String.format("Hello %s",name);
    }
}
