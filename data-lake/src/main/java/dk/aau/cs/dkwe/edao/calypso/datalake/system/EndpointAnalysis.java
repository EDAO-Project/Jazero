package dk.aau.cs.dkwe.edao.calypso.datalake.system;

import java.io.*;
import java.util.*;

public final class EndpointAnalysis
{
    private final File analysisFile;
    private final Map<Integer, Map<Integer, Map<String, Integer>>> record = new HashMap<>();  // Year -> week number -> endpoint -> count literal

    public EndpointAnalysis()
    {
        this.analysisFile = new File(Configuration.getAnalysisDir());

        if (!this.analysisFile.exists())
        {
            try
            {
                this.analysisFile.createNewFile();
            }

            catch (IOException e) {}
        }

        else
        {
            load();
        }
    }

    public EndpointAnalysis(File outputFile)
    {
        this.analysisFile = outputFile;
    }

    public void record(String endpoint, int increment)
    {
        int year = getYear(), week = getWeekNumber();

        if (!this.record.containsKey(year))
        {
            this.record.put(year, new HashMap<>());
        }

        if (!this.record.get(year).containsKey(week))
        {
            this.record.get(year).put(week, new HashMap<>());
        }

        if (!this.record.get(year).get(week).containsKey(endpoint))
        {
            this.record.get(year).get(week).put(endpoint, 0);
        }

        int old = this.record.get(year).get(week).get(endpoint);
        this.record.get(year).get(week).put(endpoint, old + increment);
        save();
    }

    private static int getYear()
    {
        return Calendar.getInstance().get(Calendar.YEAR);
    }

    private static int getWeekNumber()
    {
        return Calendar.getInstance().get(Calendar.WEEK_OF_YEAR);
    }

    private void save()
    {
        try (FileWriter writer = new FileWriter(this.analysisFile))
        {
            Comparator<Integer> sort = Comparator.comparingInt(e -> e);
            List<Integer> years = new ArrayList<>(this.record.keySet());
            years.sort(sort);

            for (Integer year : years)
            {
                writer.write("------- " + year + " -------\n");

                List<Integer> weeks = new ArrayList<>(this.record.get(year).keySet());
                weeks.sort(sort);

                for (Integer week : weeks)
                {
                    writer.write("Week " + week + "\n");

                    for (Map.Entry<String, Integer> endpoint : this.record.get(year).get(week).entrySet())
                    {
                        writer.write(endpoint.getKey() + ": " + endpoint.getValue() + "\n");
                    }
                }

                writer.write("\n");
            }

            writer.flush();
        }

        catch (IOException e) {}
    }

    // TODO: Not yet finished
    private void load()
    {

    }
}