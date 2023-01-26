package dk.aau.cs.dkwe.edao.calypso.datalake.system;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class EndpointAnalysisTest
{
    private File analysisFile = new File("analysis.test");

    @BeforeEach
    public void setup() throws IOException
    {
        this.analysisFile.createNewFile();
    }

    @AfterEach
    public void tearDown()
    {
        this.analysisFile.delete();
    }

    @Test
    public void test()
    {
        EndpointAnalysis analysis = new EndpointAnalysis(this.analysisFile);
        analysis.record("endpoint1", 3);
        analysis.record("endpoint2", 1);
        analysis.record("endpoint3", 10);

        try (BufferedReader input = new BufferedReader(new FileReader(this.analysisFile)))
        {
            String line;
            List<String> lines = new ArrayList<>();

            while ((line = input.readLine()) != null)
            {
                lines.add(line);
            }

            assertTrue(lines.size() >= 4);
            assertTrue(lines.get(0).contains("20"));
            assertTrue(lines.get(1).contains("Week"));
            assertTrue(lines.get(2).contains("endpoint"));
            assertTrue(lines.get(3).contains("endpoint"));
            assertTrue(lines.get(4).contains("endpoint"));
        }

        catch (IOException e)
        {
            Assertions.fail();
        }
    }
}
