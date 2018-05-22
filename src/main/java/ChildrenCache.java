import java.util.ArrayList;
import java.util.List;

/**
 * Created by lichen@daojia.com on 2018-5-22.
 */
public class ChildrenCache {

    protected List<String> children;



    public ChildrenCache(List<String> children) {
        this.children = children;
    }

    public ChildrenCache() {
        this.children = null;
    }

    List<String> getList() {
        return children;
    }

    List<String> addedAndSet(List<String> newChildren) {
        ArrayList<String> diff = null;

        if (children == null) {
            diff = new ArrayList<>( newChildren );
        } else {
            for (String s : newChildren) {
                if (!children.contains( s )){
                    if (diff == null ) {
                        diff = new ArrayList<>(  );
                    }
                    diff.add( s );
                }
            }
        }
        this.children = newChildren;
        return diff;
    }

    List<String> removeAndSet (List<String> newChildren) {
        List<String> diff =  null;
        if (children != null) {
            for (String s : children) {
                if (!newChildren.contains( s )){
                    if (diff == null) {
                        diff = new ArrayList<>();
                    }
                    diff.add( s );
                }
            }
        }
        this.children = newChildren;
        return diff;
    }

}
