package timely.store;

import timely.api.response.TimelyException;
import timely.configuration.Configuration;

public class DataStoreFactory {

    public static DataStore create(Configuration conf, int numWriteThreads) throws TimelyException {

        return new DataStoreImpl(conf, numWriteThreads);
    }
}
