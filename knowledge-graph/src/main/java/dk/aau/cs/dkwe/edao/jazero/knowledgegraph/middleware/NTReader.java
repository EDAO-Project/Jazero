package dk.aau.cs.dkwe.edao.jazero.knowledgegraph.middleware;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class NTReader extends RDF implements RDFReader
{
    public NTReader(File file)
    {
        super(file, FileFormat.NT);
    }

    private Set<String> readLines() throws IOException
    {
        try (BufferedReader reader = new BufferedReader(new FileReader(super.rdfFile)))
        {
            Set<String> lines = new HashSet<>();
            String line;

            while ((line = reader.readLine()) != null)
            {
                if (!line.isEmpty())
                {
                    lines.add(line);
                }
            }

            return lines;
        }
    }

    @Override
    public Set<String> readSubjects() throws IOException
    {
        return readLines()
                .stream()
                .map(line -> line.substring(1, line.indexOf('>')))
                .collect(Collectors.toSet());
    }

    @Override
    public Set<String> readPredicates() throws IOException
    {
        return readLines()
                .stream()
                .map(line -> {
                    String[] split = line.split(" ");
                    return split[1].substring(1, split[1].indexOf('>'));
                })
                .collect(Collectors.toSet());
    }

    @Override
    public Set<String> readObjects() throws IOException
    {
        return readLines()
                .stream()
                .map(line -> {
                    String[] split = line.split(" ");

                    if (split[2].contains("<"))
                    {
                        return split[2].substring(1, split[2].indexOf('>'));
                    }

                    else if (split[2].contains("\""))
                    {
                        return line.substring(line.indexOf('\"') + 1, line.lastIndexOf('\"'));
                    }

                    return split[2];
                })
                .collect(Collectors.toSet());
    }

    @Override
    public Map<String, Map<String, String>> readTriples() throws IOException
    {
        Set<String> lines = readLines();
        Map<String, Map<String, String>> triples = new HashMap<>();

        for (String line : lines)
        {
            String[] split = line.split(" ");
            String subject = split[0].substring(1, split[0].length() - 1),
                    predicate = split[1].substring(1, split[1].length() - 1),
                    object = split[2].substring(1, split[2].length() - 1);

            if (!triples.containsKey(subject))
            {
                triples.put(subject, new HashMap<>());
            }

            triples.get(subject).put(predicate, object);
        }

        return triples;
    }
}
