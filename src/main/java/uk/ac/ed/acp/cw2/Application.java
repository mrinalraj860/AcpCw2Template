package uk.ac.ed.acp.cw2;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * The Application class serves as the entry point for the Spring Boot application.
 * It is annotated with @SpringBootApplication, which is a convenience annotation
 * that adds @Configuration, @EnableAutoConfiguration, and @ComponentScan.
 * It triggers the auto-configuration of Spring Boot and scans for Spring components
 * in the package and sub-packages of this class.
 * <p>
 * The main method executes the SpringApplication.run method to launch the application.
 */
@SpringBootApplication
public class Application {

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }
}
