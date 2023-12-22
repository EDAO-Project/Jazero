package dk.aau.cs.dkwe.edao.jazero.datalake.system;

import java.io.*;
import java.util.*;

public final class EndpointAnalysis
{
    private final File analysisFile;
    private final Map<Integer, Map<Integer, Map<String, Integer>>> record = new HashMap<>();  // Year -> week number -> endpoint -> count literal

    public EndpointAnalysis()
    {
        this(new File(Configuration.getAnalysisDir()));
    }

    public EndpointAnalysis(File outputFile)
    {
        this.analysisFile = outputFile;

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

    private boolean load()
    {
        try (FileReader reader = new FileReader(this.analysisFile))
        {
            BufferedReader bufferedReader = new BufferedReader(reader);
            String line;
            int year = -1, week = -1;

            while ((line = bufferedReader.readLine()) != null)
            {
                if (line.startsWith("-------"))
                {
                    year = Integer.parseInt(line.split(" ")[1]);
                    this.record.put(year, new HashMap<>());
                }

                else if (line.startsWith("Week"))
                {
                    week = Integer.parseInt(line.split("Week ")[1]);
                    this.record.get(year).put(week, new HashMap<>());
                }

                else if (line.contains(":"))
                {
                    String[] split = line.split(":");
                    String endpoint = split[0];
                    int frequency = Integer.parseInt(split[1].substring(1));
                    this.record.get(year).get(week).put(endpoint, frequency);
                }
            }

            return true;
        }

        catch (IOException | NumberFormatException e)
        {
            return false;
        }
    }

    public Map<Integer, Map<Integer, Map<String, Integer>>> getAnalysis()
    {
        return this.record;
    }
}