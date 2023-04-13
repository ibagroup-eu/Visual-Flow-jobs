package eu.ibagroup.vf.util;

import eu.ibagroup.vf.model.EnvParam;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import java.io.InputStream;
import java.util.Optional;

/**
 * Util class for parsing and getting environmental parameters and files.
 */
@Slf4j
@UtilityClass
public class CommonUtils {

    public static final String INPUT_DATETIME_FORMAT = "yyyy-MM-dd'T'HH:mm:ss'Z'";
    public static final String OUTPUT_DATETIME_FORMAT = "E, MMM dd yyyy 'at' hh.mm a";

    public static String getEnvProperty(EnvParam envVar) {
        return Optional.of(System.getenv(envVar.toString()))
                .orElseThrow(
                        () -> new RuntimeException(String.format("Environment variable %s doesn't exist", envVar))
                );
    }

    public static InputStream getTemplate(String templatePath) {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        return classLoader.getResourceAsStream(templatePath);
    }
}
