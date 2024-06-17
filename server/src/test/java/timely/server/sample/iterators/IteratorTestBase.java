package timely.server.sample.iterators;

import java.util.ArrayList;
import java.util.Collection;

import org.apache.accumulo.core.client.sample.SamplerConfiguration;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.security.Authorizations;

public class IteratorTestBase {

    protected static class DefaultIteratorEnvironment implements IteratorEnvironment {

        AccumuloConfiguration conf;

        public DefaultIteratorEnvironment(AccumuloConfiguration conf) {
            this.conf = conf;
        }

        public DefaultIteratorEnvironment() {
            this.conf = DefaultConfiguration.getInstance();
        }

        @Override
        public IteratorScope getIteratorScope() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isFullMajorCompaction() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Authorizations getAuthorizations() {
            throw new UnsupportedOperationException();
        }

        @Override
        public IteratorEnvironment cloneWithSamplingEnabled() {
            throw new UnsupportedOperationException();
        }

        @Override
        public SamplerConfiguration getSamplerConfiguration() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isSamplingEnabled() {
            return false;
        }
    }

    protected static class CombinerIteratorEnvironment extends DefaultIteratorEnvironment {

        private IteratorScope scope;
        private boolean isFullMajc;

        CombinerIteratorEnvironment(IteratorScope scope, boolean isFullMajc) {
            this.scope = scope;
            this.isFullMajc = isFullMajc;
        }

        @Override
        public IteratorScope getIteratorScope() {
            return scope;
        }

        @Override
        public boolean isFullMajorCompaction() {
            return isFullMajc;
        }
    }

    protected static final IteratorEnvironment SCAN_IE = new CombinerIteratorEnvironment(IteratorScope.scan, false);

    protected static final Collection<ByteSequence> EMPTY_COL_FAMS = new ArrayList<ByteSequence>();

}
