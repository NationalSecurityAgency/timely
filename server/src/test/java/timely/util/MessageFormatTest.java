package timely.util;

import java.text.DecimalFormat;
import java.text.MessageFormat;
import java.text.NumberFormat;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.Assert;
import org.junit.Test;

public class MessageFormatTest {

    private static final String FMT = new String("put {0} {1,number,#} {2,number} {3} {4}");

    @Test
    public void testNumberFormat() {
        String m = "sys.cpu.user";
        long time = System.currentTimeMillis();
        double value = ThreadLocalRandom.current().nextDouble(0.0D, 100.0D);
        String put = MessageFormat.format(FMT, m, time, value, "host=localhost", "rack=r1");
        NumberFormat formattedDouble = DecimalFormat.getInstance();
        formattedDouble.setMaximumFractionDigits(3);
        String newValue = formattedDouble.format(value);
        Assert.assertEquals("put sys.cpu.user " + time + " " + newValue + " host=localhost rack=r1", put);
    }

}
