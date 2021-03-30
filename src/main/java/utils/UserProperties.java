package utils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;


//  TODO: Singleton Implementation
public class UserProperties {
    private static Properties prop;
    private static InputStream inputStream;
    private static final Logger LOG = LogManager.getLogger(UserProperties.class.getName());

    public static Properties getProps() throws IOException {
        try{
            // If already created, return the same
            if(prop!=null){
                return prop;
            }
            prop = new Properties();
            String propFileName = "config.properties";

            inputStream = UserProperties.class.getClassLoader().getResourceAsStream(propFileName);

            if (inputStream != null) {
                prop.load(inputStream);
            } else {
                LOG.error("property file '" + propFileName + "' not found in the classpath");
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            inputStream.close();
        }
        return prop;
    }
}
