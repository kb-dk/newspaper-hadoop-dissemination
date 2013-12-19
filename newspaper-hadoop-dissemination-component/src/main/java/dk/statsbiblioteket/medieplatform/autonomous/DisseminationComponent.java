package dk.statsbiblioteket.medieplatform.autonomous;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/** This is the component to generate jp2 presentation copies*/
public class DisseminationComponent {

    private static Logger log = LoggerFactory.getLogger(DisseminationComponent.class);

    /**
     * The class must have a main method, so it can be started as a command line tool
     *
     * @param args the arguments.
     *
     * @throws Exception
     * @see AutonomousComponentUtils#parseArgs(String[])
     */
    public static void main(String[] args) throws Exception {
        log.info("Starting with args {}", args);

        //Parse the args to a properties construct
        Properties properties = AutonomousComponentUtils.parseArgs(args);

        //make a new runnable component from the properties
        RunnableComponent component = new DisseminationRunnableComponent(properties);

        CallResult result = AutonomousComponentUtils.startAutonomousComponent(properties, component);
        System.out.print(result);
        System.exit(result.containsFailures());
    }
}