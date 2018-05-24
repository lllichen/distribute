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
public class Master implements Watcher {

    private static final Logger LOG = LoggerFactory.getLogger( Master.class );

    public boolean isExpired() {
        return expired;
    }

    /*
  * A master process can be either running for
  * primary master, elected primary master, or
  * not elected, in which case it is a backup
  * master.
  */
    enum MasterStates {
        RUNNING, ELECTED, NOTELECTED
    }

    private volatile MasterStates state = MasterStates.RUNNING;


    MasterStates getState() {
        return state;
    }


    private ZooKeeper zk;

    private String hostPort;

    private Random random = new Random( this.hashCode() );

    private volatile boolean connected = false;
    private volatile boolean expired = false;

    protected ChildrenCache tasksCache;
    protected ChildrenCache workersCache;

    public boolean isConnected() {
        return connected;
    }




    Master(String hostPort) {
        this.hostPort = hostPort;
    }

    void startZK() throws IOException {
        zk = new ZooKeeper( hostPort, 15000, this );
    }

    void stopZK() throws InterruptedException {
        zk.close();
    }


    @Override
    public void process(WatchedEvent event) {
        LOG.info("Processing event: " + event.toString());
        if(event.getType() == Event.EventType.None){
            switch (event.getState()) {
                case SyncConnected:
                    connected = true;
                    break;
                case Disconnected:
                    connected = false;
                    break;
                case Expired:
                    expired = true;
                    connected = false;
                    LOG.error("Session expiration");
                default:
                    break;
            }
        }
    }

    static String serviceId = Integer.toHexString( new Random().nextInt() );


    static boolean isLeader;


    StringCallback masterCreateCallback = (rc, path, ctx, name) -> {
        switch (Code.get( rc )) {
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
                LOG.error( "Something went wrong when running for master.", KeeperException.create( Code.get( rc ), path ) );
        }
    };

    void masterExists() {
        zk.exists( "/master", masterExistsWatcher, masterExistsCallback, null );
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
                if (stat == null) {
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
        if (event.getType() == Event.EventType.NodeChildrenChanged) {
            assert "/workers".equals( event.getPath() );
            getWorkers();
        }
    };

    void getWorkers() {
        zk.getChildren( "/workers", worksChangeWatcher, workersGetChildrenCallback, null );
    }

    ChildrenCallback workersGetChildrenCallback = new ChildrenCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, List<String> children) {
            switch (Code.get( rc )) {
                case CONNECTIONLOSS:
                    getWorkers();
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

    void reassingnAndSet(List<String> children) {
        List<String> toProcess;
        if (workersCache == null) {
            workersCache = new ChildrenCache( children );
            toProcess = null;
        } else {
            LOG.info( "Remove and setting" );
            toProcess = workersCache.removeAndSet( children );
        }

        if (toProcess != null) {
            for (String worker : toProcess) {
                getAbsentWorkerTasks( worker );
            }
        }
    }

    void getAbsentWorkerTasks(String worker) {
        zk.getChildren( "/assign/" + worker, false, workerAssignmentCallback, null );
    }

    ChildrenCallback workerAssignmentCallback = (rc, path, ctx, children) -> {
        switch (Code.get( rc )) {
            case CONNECTIONLOSS:
                getAbsentWorkerTasks( path );
                break;
            case OK:
                LOG.info( "Successfully got a list of assignments: " + children.size() + " tasks" );
                /**
                 * Reassign the tasks of the absent worker
                 */
                for (String task : children) {
                    getDataReassign( path + "/" + task, task );
                }
                break;
            default:
                LOG.error( "getChildren failed", KeeperException.create( Code.get( rc ), path ) );
        }
    };

    class RecreateTaskCtx {
        String path;
        String task;
        byte[] data;

        public RecreateTaskCtx(String path, String task, byte[] data) {
            this.path = path;
            this.task = task;
            this.data = data;
        }
    }


    /**
     * *********************************************
     * Recovery of tasks assigned to absent worker.*
     * *********************************************
     */

    /**
     * get reassigned task data.
     *
     * @param path Path of assigned task
     * @param task task name excluding the path prefix
     */

    void getDataReassign(String path, String task) {
        zk.getData( path, false, getDataReassignCallback, task );
    }

    DataCallback getDataReassignCallback = (rc, path, ctx, data, stat) -> {
        switch (Code.get( rc )) {
            case CONNECTIONLOSS:
                getDataReassign( path, (String) ctx );
                break;
            case OK:
                recreateTask( new RecreateTaskCtx( path, (String) ctx, data ) );
                break;
            default:
                LOG.error( "Something went wrong when getting data ", KeeperException.create( Code.get( rc ), path ) );
        }
    };


    /**
     * recreate task znode in/tasks
     *
     * @param ctx Recreate text context
     */

    void recreateTask(RecreateTaskCtx ctx) {
        zk.create( "/tasks/" + ctx.task, ctx.data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, recreateTaskCallback, ctx );
    }

    StringCallback recreateTaskCallback = (rc, path, ctx, name) -> {
        switch (Code.get( rc )) {
            case CONNECTIONLOSS:
                recreateTask( (RecreateTaskCtx) ctx );
                break;
            case OK:
                deleteAssignment( ((RecreateTaskCtx) ctx).path );
                break;
            case NODEEXISTS:
                LOG.info( "Node exists already,but if it hasn't been deleted, then it will eventually , so we keep trying: " + path );
                recreateTask( (RecreateTaskCtx) ctx );
                break;
            default:
                LOG.error( "Something went wrong when recreating task", KeeperException.create( Code.get( rc ), path ) );
        }
    };

    /**
     * Delete assignment of absent worker
     *
     * @param path Path of znode to be deleted
     */
    void deleteAssignment(String path) {
        zk.delete( path, -1, taskDeletionCallback, null );
    }

    VoidCallback taskDeletionCallback = (rc, path, rtx) -> {
        switch (Code.get( rc )) {
            case CONNECTIONLOSS:
                deleteAssignment( path );
                break;
            case OK:
                LOG.info( "Task correctly deleted: " + path );
                break;
            default:
                LOG.error( "Failed to delete task data" + KeeperException.create( Code.get( rc ), path ) );
        }
    };


    Watcher tasksChangeWatcher = event -> {
        if (event.getType() == Event.EventType.NodeChildrenChanged) {
            assert "/tasks".equals( event.getPath() );
            getTasks();
        }
    };

    void getTasks() {
        zk.getChildren( "/tasks", tasksChangeWatcher, taskGetChildrenCallback, null );
    }

    ChildrenCallback taskGetChildrenCallback = new ChildrenCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, List<String> children) {
            switch (Code.get( rc )) {
                case CONNECTIONLOSS:
                    getTasks();
                    break;
                case OK:
                    if (children != null) {
                        assignTasks( children );
                    }
                    break;
                default:
                    LOG.error( "getChildren failed.", KeeperException.create( Code.get( rc ), path ) );
            }
        }
    };

    void assignTasks(List<String> tasks) {
        for (String task : tasks)
            getTaskData( task );
    }

    void getTaskData(String task) {
        zk.getData( "/tasks/" + task, false, taskDataCallback, task );
    }

    DataCallback taskDataCallback = (rc, path, ctx, data, stat) -> {
        switch (Code.get( rc )) {
            case CONNECTIONLOSS:
                getTaskData( (String) ctx );
            case OK:
                List<String> list = workersCache.getList();
                String designatedWorker = list.get( random.nextInt( list.size() ) );
                String assignmentPath = "/assign/" + designatedWorker + "/" + (String) ctx;
                LOG.info( "Assignment path: " + assignmentPath );
                createAssignment( assignmentPath, data );
                break;
            default:
                LOG.error( "Error when trying to get task data.", KeeperException.create( Code.get( rc ), path ) );
        }
    };

    void createAssignment(String path, byte[] data) {
        zk.create( path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, assignTaskCallback, data );
    }

    StringCallback assignTaskCallback = (rc, path, ctx, name) -> {
        switch (Code.get( rc )) {
            case CONNECTIONLOSS:
                createAssignment( path, (byte[]) ctx );
            case OK:
                LOG.info( "Task assigned correctly: " + name );
                deleteTask( name.substring( name.lastIndexOf( "/" ) + 1 ) );
                break;
            case NODEEXISTS:
                LOG.warn( "Task already assigned" );
            default:
                LOG.error( "Error when trying to assign task.", KeeperException.create( Code.get( rc ), path ) );
        }
    };

    void deleteTask(String name) {
        zk.delete( "/tasks/" + name, -1, taskDeleteCallback, null );
    }

    VoidCallback taskDeleteCallback = (rc, path, ctx) -> {
        switch (Code.get( rc )) {
            case CONNECTIONLOSS:
                deleteTask( path );
                break;
            case OK:
                LOG.info( "Successfully deleted " + path );
                break;
            case NONODE:
                LOG.info( "Task has been deleted already" );
                break;
            default:
                LOG.error( "Something went wrong here, " + KeeperException.create( Code.get( rc ), path ) );
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
        zk.getData( "/master", false, masterCheckCallback, null );
    }

    void runForMaster() {
        LOG.info( serviceId );
        zk.create( "/master", serviceId.getBytes(), OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL, masterCreateCallback, null );
    }


    public void bootstrap() {
        createParent( "/workers", new byte[0] );
        createParent( "/assign", new byte[0] );
        createParent( "/tasks", new byte[0] );
        createParent( "/status", new byte[0] );
    }

    void createParent(String path, byte[] data) {
        zk.create( path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, createParentCallback, data );
    }

    StringCallback createParentCallback = (rc, path, ctx, name) -> {
        LOG.info( "name is : ", name );

        switch (Code.get( rc )) {
            case CONNECTIONLOSS:
                createParent( path, (byte[]) ctx );
                break;
            case OK:
                LOG.info( "Parent created" );
                break;
            case NODEEXISTS:
                LOG.warn( "Parent already registered: " + path );
                break;
            default:
                LOG.error( "Something went wrong: ", KeeperException.create( Code.get( rc ), path ) );
        }
    };

    public static void main(String[] args) throws Exception {
        Master m = new Master( args[0] );
        m.startZK();
        while (!m.isConnected()) {
            Thread.sleep( 100 );
        }

        /**
         * bootstrap() creates some necessary znodes
         */
        m.bootstrap();

        /**
         * now runs for master
         */
        m.runForMaster();

        while (!m.isExpired()) {
            Thread.sleep( 1000 );
        }

        m.stopZK();
    }
}
