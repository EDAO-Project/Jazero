package dk.aau.cs.dkwe.edao.jazero.knowledgegraph.middleware;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

public class NTReaderTest
{
    private final File file = new File("src/test/resources/test.nt");

    @Test
    public void testReadSubjects()
    {
        Set<String> gtSubjects = Set.of("http://www.w3.org/2001/sw/RDFCore/ntriples/");
        RDFReader reader = new NTReader(this.file);
        assertDoesNotThrow(() -> {
            Set<String> subjects = reader.readSubjects();

            for (String subject : subjects)
            {
                assertTrue(gtSubjects.contains(subject));
            }
        });
    }

    @Test
    public void testReadPredicates()
    {
        Set<String> gtPredicates = Set.of("http://purl.org/dc/elements/1.1/creator", "http://purl.org/dc/elements/1.1/publisher");
        RDFReader reader = new NTReader(this.file);
        assertDoesNotThrow(() -> {
            Set<String> predicates = reader.readPredicates();

            for (String predicate : predicates)
            {
                assertTrue(gtPredicates.contains(predicate));
            }
        });
    }

    @Test
    public void testReadObjects()
    {
        Set<String> gtObjects = Set.of("Dave Beckett", "Art Barstow", "http://www.w3.org/");
        RDFReader reader = new NTReader(this.file);
        assertDoesNotThrow(() -> {
            Set<String> objects = reader.readObjects();

            for (String object : objects)
            {
                assertTrue(gtObjects.contains(object));
            }
        });
    }

    @Test
    public void testReadTriples()
    {
        Set<String> gtSubjects = Set.of("http://www.w3.org/2001/sw/RDFCore/ntriples/"),
                gtPredicates = Set.of("http://purl.org/dc/elements/1.1/creator", "http://purl.org/dc/elements/1.1/publisher");
        RDFReader reader = new NTReader(this.file);
        assertDoesNotThrow(() -> {
            Map<String, Map<String, String>> triples = reader.readTriples();

            for (Map.Entry<String, Map<String, String>> entity : triples.entrySet())
            {
                assertTrue(gtSubjects.contains(entity.getKey()));

                for (Map.Entry<String, String> relationship : entity.getValue().entrySet())
                {
                    assertTrue(gtPredicates.contains(relationship.getKey()));
                }
            }

            assertEquals("http://www.w3.org/", triples.get("http://www.w3.org/2001/sw/RDFCore/ntriples/").get("http://purl.org/dc/elements/1.1/publisher"));
        });
    }
}
