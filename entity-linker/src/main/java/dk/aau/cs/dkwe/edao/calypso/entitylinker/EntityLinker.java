package dk.aau.cs.dkwe.edao.calypso.entitylinker;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.bind.annotation.RestController;

@SpringBootApplication
@RestController
public class EntityLinker
{
    public static void main(String[] args)
    {
        SpringApplication.run(EntityLinker.class, args);
    }
}