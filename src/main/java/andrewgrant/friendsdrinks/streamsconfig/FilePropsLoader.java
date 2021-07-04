package andrewgrant.friendsdrinks.streamsconfig;

import java.io.FileInputStream;
import java.io.IOException;

/**
 * Helper class for retrieving properties.
 */
public class FilePropsLoader {

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

