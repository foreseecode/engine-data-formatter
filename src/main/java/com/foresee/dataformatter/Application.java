package com.foresee.dataformatter;

import com.google.common.base.Stopwatch;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;

/**
 * @author dustin.benac
 * @since 2/13/2017
 */
@SpringBootApplication
@Slf4j
public class Application {

    public static void main(String[] args) throws Exception {
        ConfigurableApplicationContext ctx = new SpringApplicationBuilder(Application.class)
                .web(false)
                .application()
                .run(args);

        log.debug("Starting...");
        Stopwatch stopwatch = Stopwatch.createStarted();

        ctx.getBean(DataFormatter.class).execute();

        log.debug("Took {} to complete", stopwatch);
        ctx.close();
        System.exit(0);
    }

}
