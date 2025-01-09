package dk.aau.cs.dkwe.edao.jazero.knowledgegraph.middleware;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class TTLReader extends RDF implements RDFReader
{
    public TTLReader(File file)
    {
        super(file, FileFormat.TTL);
    }

    @Override
    public Set<String> readSubjects() throws IOException
    {
        Set<String> subjects = new HashSet<>();
        BufferedReader reader = new BufferedReader(new FileReader(super.rdfFile));
        String line;

        while ((line = reader.readLine()) != null)
        {
            if (line.startsWith("<"))
            {
                String subject = line.substring(1, line.indexOf('>'));
                subjects.add(subject);
            }
        }

        return subjects;
    }

    @Override
    public Set<String> readPredicates() throws IOException
    {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Set<String> readObjects() throws IOException
    {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Map<String, Map<String, String>> readTriples() throws IOException
    {
        throw new UnsupportedOperationException("Not supported yet.");
    }
}
