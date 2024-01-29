package timely.store;

import org.apache.accumulo.core.client.AccumuloClient;

import timely.api.response.TimelyException;
import timely.configuration.Configuration;

public class DataStoreFactory {

    public static DataStore create(Configuration conf, AccumuloClient accumuloClient, int numWriteThreads) throws TimelyException {

        return new DataStoreImpl(conf, accumuloClient, numWriteThreads);
    }
}
