import org.apache.zookeeper.*;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.KeeperException.ConnectionLossException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Random;

import static org.apache.zookeeper.AsyncCallback.*;
import static org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE;

/**
 * Created by lichen@daojia.com on 2018-5-10.
 */
public class Master implements Watcher{

    private static final Logger LOG = LoggerFactory.getLogger( Master.class );

    static ZooKeeper zk;

    String hostPort;

    Master(String hostPort){
        this.hostPort = hostPort;
    }

    void startZK() throws IOException {
        zk = new ZooKeeper( hostPort,15000,this );
    }

    void stopZK() throws InterruptedException {
        zk.close();
    }


    @Override
    public void process(WatchedEvent event) {
        System.out.println(event);
    }

    static String serviceId = Integer.toHexString( new Random( ).nextInt() );


    static boolean isLeader ;

    StringCallback masterCreateCallback = new StringCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (Code.get(rc)) {
                case CONNECTIONLOSS:
                    checkMaster();
                    return;
                case OK:
                    isLeader = true;
                    break;
                default:
                    isLeader = false;
            }
            System.out.println("I'm " + (isLeader? "": "not ") + "the leader");
        }
    };


    DataCallback masterCheckCallback = new DataCallback() {

        @Override
        public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
            switch (Code.get( rc )) {
                case CONNECTIONLOSS:
                    checkMaster();
                    return;
                case NONODE:
                    runForMaster();
                    return;
            }

        }
    };

     void checkMaster() {
          zk.getData( "/master", false, masterCheckCallback,null );
    }

    void runForMaster() {
        LOG.info( serviceId );
        zk.create( "/master", serviceId.getBytes(),OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL ,masterCreateCallback, null);
    }


    public void bootstrap() {
         createParent("/workers",new byte[0]);
         createParent("/assign",new byte[0]);
         createParent("/tasks",new byte[0]);
         createParent("/status",new byte[0]);
    }

    void createParent(String path, byte[] data) {
         zk.create( path,data, ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT,createParentCallback,data );
    }

    StringCallback createParentCallback = new StringCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, String name) {
            LOG.info("name is : ",name );

            switch (Code.get( rc )) {
                case CONNECTIONLOSS:
                    createParent( path,(byte[]) ctx );
                    break;
                case OK:
                    LOG.info("Parent created");
                    break;
                case NODEEXISTS:
                    LOG.warn( "Parent already registered: " + path );
                    break;
                default:
                    LOG.error( "Something went wrong: ", KeeperException.create( Code.get( rc ),path ) );
            }
        }
    };

    public static void main(String[] args) throws Exception {
        Master m = new Master( args[0] );
        m.startZK();
        m.bootstrap();
        m.runForMaster();

        if(isLeader){
            System.out.println("I'm the leader");
//            wait for a bit
            Thread.sleep( 60000 );
        }else {
            System.out.println("Someone else is the leader");
        }
//
        m.stopZK();
    }
}
