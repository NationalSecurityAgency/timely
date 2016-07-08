package timely.test.integration;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import timely.Server;
import timely.test.IntegrationTest;

@Category(IntegrationTest.class)
public class StaticFileServerIT extends OneWaySSLBase {

    @Test(expected = NotSuccessfulException.class)
    public void testGetFavIconRequest() throws Exception {
        final Server s = new Server(conf);
        try {
            query("https://127.0.0.1:54322/favicon.ico", 404, "application/json");
        } finally {
            s.shutdown();
        }
    }

    @Test(expected = NotSuccessfulException.class)
    public void testGetBadPath() throws Exception {
        final Server s = new Server(conf);
        try {
            query("https://127.0.0.1:54322/index.html", 403, "application/json");
        } finally {
            s.shutdown();
        }
    }

    @Test(expected = NotSuccessfulException.class)
    public void testGetGoodPath() throws Exception {
        final Server s = new Server(conf);
        try {
            query("https://127.0.0.1:54322/webapp/test.html", 404, "application/json");
        } finally {
            s.shutdown();
        }
    }

}
