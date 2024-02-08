package timely.test.integration;

import java.io.File;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.netty.handler.ssl.util.SelfSignedCertificate;
import timely.auth.AuthCache;

public class TwoWaySSLFailureIT extends TwoWaySSLBase {

    private static File clientTrustStoreFile;
    private static SelfSignedCertificate serverCert;

    static {
        try {
            serverCert = new SelfSignedCertificate("CN=bad.example.com");
            clientTrustStoreFile = serverCert.certificate().getAbsoluteFile();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public SelfSignedCertificate getServerCert() {
        return serverCert;
    }

    protected File getClientTrustStoreFile() {
        return clientTrustStoreFile;
    }

    @Before
    public void setup() throws Exception {
        accumuloClient.tableOperations().list().forEach(t -> {
            if (t.startsWith("timely")) {
                try {
                    accumuloClient.tableOperations().delete(t);
                } catch (Exception e) {}
            }
        });
        startServer();
    }

    @After
    public void tearDown() throws Exception {
        AuthCache.resetConfiguration();
        stopServer();
    }

    @Test(expected = UnauthorizedUserException.class)
    public void testBasicAuthLoginFailure() throws Exception {
        String metrics = "https://localhost:54322/api/metrics";
        query(metrics);
    }

}
