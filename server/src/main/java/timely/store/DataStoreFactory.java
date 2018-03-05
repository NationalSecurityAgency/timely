package timely.store;

import timely.Configuration;
import timely.api.response.TimelyException;

public class DataStoreFactory {

    public static DataStore create(Configuration conf, int numWriteThreads) throws TimelyException {

        return new DataStoreImpl(conf, numWriteThreads);
    }
}
