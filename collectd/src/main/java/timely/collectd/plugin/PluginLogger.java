package timely.collectd.plugin;

import org.slf4j.event.Level;

public interface PluginLogger {

    void log(Level level, String s);
}
