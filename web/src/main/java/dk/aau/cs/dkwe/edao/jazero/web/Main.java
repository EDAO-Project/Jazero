package dk.aau.cs.dkwe.edao.jazero.web;

import com.vaadin.flow.component.page.AppShellConfigurator;
import com.vaadin.flow.component.page.Push;
import com.vaadin.flow.server.AppShellSettings;
import com.vaadin.flow.theme.Theme;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import java.time.Clock;

/**
 * The entry point of the Spring Boot application.
 *
 * Use the @PWA annotation make the application installable on phones, tablets
 * and some desktop browsers.
 *
 */
@SpringBootApplication
@Push
@Theme("search-theme")
public class Main implements AppShellConfigurator
{
    @Bean
    public Clock clock()
    {
        return Clock.systemUTC();
    }

    @Override
    public void configurePage(AppShellSettings settings)
    {
        settings.addFavIcon("icon", "images/logo.png", "16x16");
    }

    public static void main(String[] args)
    {
        SpringApplication.run(Main.class, args);
    }
}
