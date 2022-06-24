package dk.aau.cs.dkwe.edao.calypso.datalake;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.bind.annotation.RestController;

@SpringBootApplication
@RestController
public class DataLake
{
    public static void main(String[] args)
    {
        SpringApplication.run(DataLake.class, args);
    }
}