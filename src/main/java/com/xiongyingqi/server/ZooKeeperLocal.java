package com.xiongyingqi.server;


import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

/**
 * @author xiongyingqi
 * @since 2016-11-12 18:29
 */
public class ZooKeeperLocal {

    ZooKeeperServerMain zooKeeperServer;
    private Thread thread;

    public ZooKeeperLocal(Properties zkProperties) throws FileNotFoundException, IOException {


        QuorumPeerConfig quorumConfiguration = new QuorumPeerConfig();
        try {
            quorumConfiguration.parseProperties(zkProperties);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        zooKeeperServer = new ZooKeeperServerMain();
        final ServerConfig configuration = new ServerConfig();
        configuration.readFrom(quorumConfiguration);


        thread = new Thread() {
            public void run() {
                try {
                    zooKeeperServer.runFromConfig(configuration);
                } catch (IOException e) {
                    System.out.println("ZooKeeper Failed");
                    e.printStackTrace();
                }
            }
        };
        thread.setDaemon(true);
        thread.start();
        Runtime.getRuntime().addShutdownHook(new Thread(){
            @Override
            public void run() {
                thread.interrupt();
            }
        });
    }

    public void stop(){
        if (thread.isAlive()) {
            thread.interrupt();
        }
    }

}
