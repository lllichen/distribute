import org.apache.zookeeper.*;

import static org.apache.zookeeper.KeeperException.*;
import static org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE;

/**
 * Created by lichen@daojia.com on 2018-5-17.
 */
public class Client implements Watcher{

    ZooKeeper zk;
    String hostPort;
    String name;

    public Client(String hostPort) {
        this.hostPort = hostPort;
    }

    void startZK() throws Exception {
        zk = new ZooKeeper( hostPort,15000,this );
    }

    String queueCommand(String command) throws Exception {
        while (true) {
            try {
                String name = zk.create( "/task/task-", command.getBytes(),OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL );
                return name;
            }catch (NodeExistsException e){
                throw new Exception( name + " already appears to running" );
            }catch (ConnectionLossException e){

            }
        }
    }

    @Override
    public void process(WatchedEvent event) {
        System.out.println(event);
    }

    public static void main(String[] args) throws Exception {
        Client c = new Client( args[0] ) ;
        c.startZK();
        String name = c.queueCommand( args[1] );
        System.out.println("Created "+ name);
    }
}
