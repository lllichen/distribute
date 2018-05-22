import org.apache.zookeeper.*;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.KeeperException.ConnectionLossException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Random;

import static org.apache.zookeeper.AsyncCallback.*;
import static org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE;

/**
 * Created by lichen@daojia.com on 2018-5-10.
 */
public class Master implements Watcher{

    private static final Logger LOG = LoggerFactory.getLogger( Master.class );


    /*
     * A master process can be either running for
     * primary master, elected primary master, or
     * not elected, in which case it is a backup
     * master.
     */
    enum MasterStates {RUNNING, ELECTED, NOTELECTED};

    private volatile MasterStates state = MasterStates.RUNNING;

    static ZooKeeper zk;

    String hostPort;

    private Random random = new Random(this.hashCode());

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


    StringCallback masterCreateCallback = (rc, path, ctx, name) -> {
        switch (Code.get(rc)) {
            case CONNECTIONLOSS:
                checkMaster();
                return;
            case OK:
                state = MasterStates.ELECTED;
                takeLeadership();
                break;
            case NODEEXISTS:
                state = MasterStates.NOTELECTED;
                masterExists();
            default:
                state = MasterStates.NOTELECTED;
                LOG.error( "Something went wrong when running for master.", KeeperException.create( Code.get( rc ),path ) );
        }
    };

    void masterExists() {
        zk.exists( "/master", masterExistsWatcher,masterExistsCallback,null );
    }


    Watcher masterExistsWatcher = new Watcher() {
        @Override
        public void process(WatchedEvent event) {
            if (event.getType() == Event.EventType.NodeDeleted) {
                assert "/master".equals( event.getPath() );
                runForMaster();
            }
        }
    };

    StatCallback masterExistsCallback = (rc, path, ctx, stat) -> {
        switch (Code.get( rc )) {
            case CONNECTIONLOSS:
                masterExists();
                break;
            case OK:
                if(stat == null) {
                    state = MasterStates.RUNNING;
                    runForMaster();
                }
                break;
            default:
                checkMaster();
                break;
        }
    };

    private void takeLeadership() {
    }

    Watcher worksChangeWatcher = event -> {
        if( event.getType() == Event.EventType.NodeChildrenChanged) {
            assert "/workers".equals( event.getPath() );
            getWorkers();
        }
    };

    void getWorkers() {
        zk.getChildren( "/workers", worksChangeWatcher,workersGetChildrenCallback, null );
    }

    ChildrenCallback workersGetChildrenCallback = new ChildrenCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, List<String> children) {
            switch (Code.get( rc )) {
                case CONNECTIONLOSS:
                    getWorkerList();
                    break;
                case OK:
                    LOG.info( "Successfully goot a list of workers: " + children.size() + " workers" );
                    reassingnAndSet( children );
                    break;
                default:
                    LOG.error( "getChildren failed", KeeperException.create( Code.get( rc ), path ) );
            }
        }
    };

    ChildrenCache workersCache;

    void reassingnAndSet(List<String> children) {
        List<String> toProcess;
        if(workersCache == null) {
            workersCache = new ChildrenCache( children );
            toProcess = null;
        } else {
            LOG.info( "Remove and setting" );
            toProcess = workersCache.removeAndSet( children );
        }

        if (toProcess != null) {
            for (String worker : toProcess) {
                getAbsentWorkerTasks(worker);
            }
        }
    }


    Watcher tasksChangeWatcher = new Watcher() {
        @Override
        public void process(WatchedEvent event) {
            if (event.getType() == Event.EventType.NodeChildrenChanged) {
                assert "/tasks".equals( event.getPath() );
                getTasks();
            }
        }
    };

    void getTasks() {
        zk.getChildren( "/tasks",tasksChangeWatcher,taskGetChildrenCallback,null );
    }

    ChildrenCallback taskGetChildrenCallback = new ChildrenCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, List<String> children) {
            switch (Code.get( rc )) {
                case CONNECTIONLOSS:
                    getTasks();
                    break;
                case OK:
                    if (children != null ) {
                        assignTasks(children);
                    }
                    break;
                    default:
                        LOG.error( "getChildren failed." KeeperException.create( Code.get(rc),path ) );
            }
        }
    };

    void assignTasks(List<String> tasks) {
        for (String task : tasks)
            getTaskData(task);
    }

    void getTaskData(String task) {
        zk.getData( "/tasks/"+task,false,taskDataCallback,task );
    }

    DataCallback taskDataCallback = (rc, path, ctx, data, stat) -> {
        switch (Code.get( rc )) {
            case CONNECTIONLOSS:
                getTaskData( (String)ctx );
            case OK:
                List<String> list = workersCache.getList();
                String designatedWorker = list.get( random.nextInt(list.size()) );
                String assignmentPath = "/assign/"+designatedWorker+"/"+(String)ctx;
                LOG.info( "Assignment path: "+ assignmentPath );
                createAssignment(assignmentPath,data);
                break;
            default:
                LOG.error( "Error when trying to get task data.", KeeperException.create( Code.get( rc ),path ));
        }
    };

    void createAssignment(String path, byte[] data) {
        zk.create( path,data, ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT,assignTaskCallback, data );
    }

    StringCallback assignTaskCallback = new StringCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (Code.get( rc )) {
                case CONNECTIONLOSS:
                    createAssignment( path,(byte[]) ctx );
                case OK:
                    LOG.info( "Task assigned correctly: " + name );
                    deleteTask(name.substring( name.lastIndexOf( "/" )+1 ));
                    break;
                case NODEEXISTS:
                    LOG.warn( "Task already assigned" );
                default:
                    LOG.error( "Error when trying to assign task.", KeeperException.create( Code.get( rc ),path ) );
            }
        }
    };

    DataCallback masterCheckCallback = (rc, path, ctx, data, stat) -> {
        switch (Code.get( rc )) {
            case CONNECTIONLOSS:
                checkMaster();
                return;
            case NONODE:
                runForMaster();
                return;
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

    StringCallback createParentCallback = (rc, path, ctx, name) -> {
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
        Thread.sleep( 60000 );
        m.stopZK();
    }
}
