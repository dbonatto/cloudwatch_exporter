package io.prometheus.cloudwatch;


import com.amazonaws.util.StringUtils;

import java.util.logging.Level;
import java.util.logging.Logger;

public class Configuration {

    private static final Logger LOGGER = Logger.getLogger(CloudWatchCollector.class.getName());

    private static final String CLOUDWATCH_THREADS = "CLOUDWATCH_THREADS";
    public static final int CLOUDWATCH_THREADS_DEFAULT = 10;

    private static final String CLOUDWATCH_MAXCONNECTIONS = "CLOUDWATCH_MAXCONNECTIONS";
    public static final int CLOUDWATCH_MAXCONNECTIONS_DEFAULT = 150;

    public static int maxConnections() {
        return getVariableOrDefaultInt(CLOUDWATCH_MAXCONNECTIONS, CLOUDWATCH_MAXCONNECTIONS_DEFAULT);
    }

    public static int threadPoolSize() {
        return getVariableOrDefaultInt(CLOUDWATCH_THREADS, CLOUDWATCH_THREADS_DEFAULT);
    }

    private static int getVariableOrDefaultInt(String label, int defaultValue) {
        int retVal = defaultValue;

        String value = System.getenv(label);

        if (!StringUtils.isNullOrEmpty(value)) {
            try {
                retVal = Integer.parseInt(value);
            } catch (NumberFormatException e) {
                LOGGER.log(Level.WARNING, "Could not parse configuration '" + label + "' with value '" + value + "'", e);
            }
        }

        return retVal;
    }
}
