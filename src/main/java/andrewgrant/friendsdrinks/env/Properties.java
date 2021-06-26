package andrewgrant.friendsdrinks.env;

import java.io.FileInputStream;
import java.io.IOException;

/**
 * Helper class for retrieving properties.
 */
public class Properties {

    public static final String STREAMS_GROUP_INSTANCE_ID = "group.instance.id";
    private static final String STREAMS_GROUP_INSTANCE_ID_ENV_VAR = "STREAMS_GROUP_INSTANCE_ID";

    public static java.util.Properties load(String fileName) throws IOException {
        java.util.Properties envProps = new java.util.Properties();
        FileInputStream input = new FileInputStream(fileName);
        try {
            envProps.load(input);
        } finally {
            input.close();
        }
        if (System.getenv(STREAMS_GROUP_INSTANCE_ID_ENV_VAR) != null) {
            envProps.setProperty(STREAMS_GROUP_INSTANCE_ID, System.getenv(STREAMS_GROUP_INSTANCE_ID_ENV_VAR));
        }
        return envProps;
    }
}

