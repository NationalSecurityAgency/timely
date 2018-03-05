package timely.store.cache;

import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.security.VisibilityEvaluator;
import org.apache.accumulo.core.security.VisibilityParseException;
import org.apache.accumulo.core.util.BadArgumentException;
import org.apache.commons.collections.map.LRUMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VisibilityFilter {

    protected VisibilityEvaluator ve;
    protected LRUMap cache;

    private static final Logger log = LoggerFactory
            .getLogger(org.apache.accumulo.core.iterators.system.VisibilityFilter.class);

    public VisibilityFilter(Authorizations authorizations) {
        this.ve = new VisibilityEvaluator(authorizations);
        this.cache = new LRUMap(1000);
    }

    protected boolean accept(ColumnVisibility visibility) {

        Boolean b = (Boolean) cache.get(visibility);
        if (b != null)
            return b;

        try {
            Boolean bb = ve.evaluate(visibility);
            cache.put(visibility, bb);
            return bb;
        } catch (VisibilityParseException | BadArgumentException e) {
            log.error("Parse Error", e);
            return false;
        }
    }
}