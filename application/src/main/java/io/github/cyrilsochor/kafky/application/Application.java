package io.github.cyrilsochor.kafky.application;

import io.github.cyrilsochor.kafky.core.config.ConfigurationManager;
import io.github.cyrilsochor.kafky.core.config.KafkyConfiguration;
import io.github.cyrilsochor.kafky.core.runtime.KafkyRuntime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;

@SuppressWarnings("java:S106")
public class Application {

    private static final Logger LOG = LoggerFactory.getLogger(Application.class);

    static {
        SLF4JBridgeHandler.removeHandlersForRootLogger();
        SLF4JBridgeHandler.install();
    }

    public static void main(String[] args) {
        if (args.length == 0) {
            System.err.println("Usage: kafky configuration-file-1 [configuration-file-2...]");
            System.exit(10);
        }

        LOG.debug("Start");

        try {

            KafkyConfiguration cfg = null;
            for (final String arg : args) {
                final KafkyConfiguration argCfg = ConfigurationManager.readFile(arg);
                if (cfg == null) {
                    cfg = argCfg;
                } else {
                    ConfigurationManager.merge(cfg, argCfg);
                }
            }

            final KafkyRuntime runtime = new KafkyRuntime();
            runtime.run(cfg);
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            System.err.println("ERROR " + e.getClass().getSimpleName() + (e.getMessage() == null ? "" : ": " + e.getMessage()));
            System.exit(1);
        }

        LOG.debug("Success");
    }

}
