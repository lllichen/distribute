import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Random;

import static org.apache.zookeeper.AsyncCallback.*;
import static org.apache.zookeeper.KeeperException.*;
import static org.apache.zookeeper.ZooDefs.*;

/**
 * Created by lichen@daojia.com on 2018-5-17.
 */
public class Worker implements Watcher{

    private static final Logger LOG = LoggerFactory.getLogger( Worker.class );

    ZooKeeper zk ;

    String hostPort;

    Random random = new Random( );

    String serviceId = Integer.toHexString( random.nextInt() );

    public Worker(String hostPort) {
        this.hostPort = hostPort;
    }

    void startZK() throws IOException {
        zk = new ZooKeeper( hostPort,15000, this );
    }

    @Override
    public void process(WatchedEvent event) {
        LOG.info( event.toString()+", " + hostPort );
    }

    void register() {
        zk.create( "/workers/worker-"+serviceId,"Idle".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL,createWorkerCallback, null );
    }

    StringCallback createWorkerCallback = new StringCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, String name) {
            switch(Code.get(rc)) {
                case CONNECTIONLOSS:
                    register();
                    break;
                case OK:
                    LOG.info( "Registered successfully: " +serviceId );
                    break;
                case NODEEXISTS:
                    LOG.warn( "Already registered: " + serviceId );
                    break;
                    default:
                        LOG.error( "Something went wrong: "+ KeeperException.create( Code.get( rc ), path ) );
            }
        }
    };


    StatCallback statusUpdateCallback = new StatCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, Stat stat) {
            switch (Code.get(rc)) {
                case CONNECTIONLOSS:
                    updateStatus((String) ctx);
                    return;
            }
        }
    };

    String status;

    String name;

    synchronized private void updateStatus (String status) {
        if (status == this.status) {
            zk.setData( "/workers/"+name,status.getBytes(),-1,statusUpdateCallback,status );
        }
    }

    public void setStatus(String status) {
        this.status = status;
        updateStatus( status );
    }

    public static void main(String[] args) throws Exception {
        Worker w = new Worker( args[0] );
        w.startZK();
        w.register();
        Thread.sleep( 30000 );
    }
}
