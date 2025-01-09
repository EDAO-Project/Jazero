package dk.aau.cs.dkwe.edao.jazero.knowledgegraph.middleware;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

public interface RDFReader
{
    Set<String> readSubjects() throws IOException;
    Set<String> readPredicates() throws IOException;
    Set<String> readObjects() throws IOException;
    Map<String, Map<String, String>> readTriples() throws IOException;
}