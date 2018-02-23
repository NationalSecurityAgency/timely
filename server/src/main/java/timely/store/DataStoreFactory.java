package timely.store;

import timely.Configuration;
import timely.api.response.TimelyException;
import timely.store.memory.DataStoreCache;

public class DataStoreFactory {

    public static DataStore create(Configuration conf, int numWriteThreads, DataStoreCache cache)
            throws TimelyException {

        return new DataStoreImpl(conf, numWriteThreads, cache);
    }
}
