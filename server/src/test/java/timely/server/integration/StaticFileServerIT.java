package timely.server.integration;

import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

import timely.common.configuration.HttpProperties;
import timely.test.IntegrationTest;
import timely.test.TimelyTestRule;

@Category(IntegrationTest.class)
@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@ActiveProfiles({"oneWaySsl"})
public class StaticFileServerIT extends OneWaySSLBase {

    private String baseUrl;

    @Autowired
    @Rule
    public TimelyTestRule testRule;

    @Autowired
    private HttpProperties httpProperties;

    @Before
    public void setup() {
        super.setup();
        this.baseUrl = "https://" + httpProperties.getHost() + ":" + httpProperties.getPort();
    }

    @After
    public void cleanup() {
        super.cleanup();
    }

    @Test(expected = NotSuccessfulException.class)
    public void testGetFavIconRequest() throws Exception {
        query(baseUrl + "/favicon.ico", 404, "application/json");
    }

    @Test(expected = NotSuccessfulException.class)
    public void testGetBadPath() throws Exception {
        query(baseUrl + "/index.html", 403, "application/json");
    }

    @Test(expected = NotSuccessfulException.class)
    public void testGetGoodPath() throws Exception {
        query(baseUrl + "/webapp/test.html", 404, "application/json");
    }
}
