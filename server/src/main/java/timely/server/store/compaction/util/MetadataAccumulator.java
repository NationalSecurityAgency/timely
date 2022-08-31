package timely.server.store.compaction.util;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.io.Text;

import timely.server.store.compaction.TabletRowAdapter;

public class MetadataAccumulator {

    private final List<Entry> list;
    private Entry currentEntry;

    public MetadataAccumulator() {
        this.list = new LinkedList<>();
    }

    public Entry checkEntryState(Text tablet) {
        // entry may be null on initial invocation or after the pushOrDiscard operation
        // check to see if the tablet has changed, if so then
        // push or discard the current state
        if (currentEntry != null && !tablet.equals(currentEntry.getTablet())) {
            pushOrDiscardEntry();
        }
        if (currentEntry == null) {
            currentEntry = new Entry(tablet);
        }

        return currentEntry;
    }

    public Collection<Entry> getEntries() {
        return Collections.unmodifiableCollection(list);
    }

    public void ensureState() {
        pushOrDiscardEntry();
    }

    private void pushOrDiscardEntry() {
        if (currentEntry != null && currentEntry.hasFiles()) {
            list.add(currentEntry);
        }
        currentEntry = null;
    }

    public static class Entry {

        private final Text tablet;
        private final String tabletPrefix;
        private final boolean hasTabletOffset;
        private final long tabletOffset;
        private Text tabletPrev;
        private long milliseconds;
        private long totalBytes;
        private int totalFiles;

        public Entry(Text tablet) {
            this.tablet = tablet;
            Optional<String> prefixOptional = TabletRowAdapter.decodeRowPrefix(tablet);
            OptionalLong offsetOptional = TabletRowAdapter.decodeRowOffset(tablet);
            tabletPrefix = prefixOptional.orElseGet(tablet::toString);
            if (offsetOptional.isPresent()) {
                hasTabletOffset = true;
                tabletOffset = offsetOptional.getAsLong();
            } else {
                hasTabletOffset = false;
                tabletOffset = -1;
            }
        }

        public String getTabletPrefix() {
            return tabletPrefix;
        }

        public Text getTablet() {
            return tablet;
        }

        public Text getTabletPrev() {
            return tabletPrev;
        }

        public long getMilliseconds() {
            return milliseconds;
        }

        public long getTotalFileBytes() {
            return totalBytes;
        }

        public int getTotalFiles() {
            return totalFiles;
        }

        public OptionalLong getTabletOffset() {
            return hasTabletOffset ? OptionalLong.of(tabletOffset) : OptionalLong.empty();
        }

        public boolean hasFiles() {
            return totalFiles > 0;
        }

        public void addFile(long size) {
            totalFiles++;
            totalBytes += size;
        }

        public void setTablePrev(Text prev) {
            tabletPrev = prev;
        }

        public void setMilliseconds(long val) {
            milliseconds = val;
        }

        @Override
        public String toString() {
            return String.format("metadata.entry {tablet: %s, millis: %d, offset-ms: %d, age-days: %d, files %d}", tablet, milliseconds, tabletOffset,
                            (hasTabletOffset ? TimeUnit.MILLISECONDS.toDays(System.currentTimeMillis() - tabletOffset) : -1), totalFiles);
        }
    }
}
