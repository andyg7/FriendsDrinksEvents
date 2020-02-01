package andrewgrant.friendsdrinks.env;

import java.io.FileInputStream;
import java.io.IOException;

/**
 * Helper class for retrieving properties.
 */
public class Properties {

    public static java.util.Properties load(String fileName) throws IOException {
        java.util.Properties envProps = new java.util.Properties();
        FileInputStream input = new FileInputStream(fileName);
        try {
            envProps.load(input);
        } finally {
            input.close();
        }
        return envProps;
    }
}

