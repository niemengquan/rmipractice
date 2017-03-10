package com.nmq.rmi;

import java.rmi.Remote;
import java.rmi.RemoteException;

/**
 * Created by niemengquan on 2017/3/10.
 */
public interface HelloService extends Remote {
    String sayHello(String name) throws RemoteException;
}
