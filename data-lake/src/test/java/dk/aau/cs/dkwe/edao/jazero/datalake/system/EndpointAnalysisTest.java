package dk.aau.cs.dkwe.edao.jazero.datalake.system;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
    public void testWrite()
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

    @Test
    public void testLoad()
    {
        EndpointAnalysis analysis1 = new EndpointAnalysis(this.analysisFile);
        analysis1.record("endpoint1", 3);
        analysis1.record("endpoint2", 1);
        analysis1.record("endpoint3", 10);

        EndpointAnalysis analysis2 = new EndpointAnalysis(this.analysisFile);
        var record = analysis2.getAnalysis();
        int thisYear = Calendar.getInstance().get(Calendar.YEAR), thisWeek = Calendar.getInstance().get(Calendar.WEEK_OF_YEAR);
        assertEquals(1, record.size());
        assertTrue(record.containsKey(thisYear));
        assertEquals(1, record.get(thisYear).size());
        assertTrue(record.get(thisYear).containsKey(thisWeek));

        // Check registered endpoints
        Map<String, Integer> endpoints = record.get(thisYear).get(thisWeek);
        assertTrue(endpoints.containsKey("endpoint1"));
        assertTrue(endpoints.containsKey("endpoint2"));
        assertTrue(endpoints.containsKey("endpoint3"));
        assertEquals(3, endpoints.get("endpoint1"));
        assertEquals(1, endpoints.get("endpoint2"));
        assertEquals(10, endpoints.get("endpoint3"));
    }
}
