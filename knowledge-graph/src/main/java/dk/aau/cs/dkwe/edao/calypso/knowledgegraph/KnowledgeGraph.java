package dk.aau.cs.dkwe.edao.calypso.knowledgegraph;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.bind.annotation.RestController;

@SpringBootApplication
@RestController
public class KnowledgeGraph
{
    public static void main(String[] args)
    {
        SpringApplication.run(KnowledgeGraph.class, args);
    }
}