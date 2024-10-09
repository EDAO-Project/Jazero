package dk.aau.cs.dkwe.edao.jazero.datalake.system;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Date;

public class FileLogger
{
    public enum Service
    {
        SDL_Manager("SDL Manager"),
        EKG_Manager("EKG Manager"),
        EntityLinker_Manager("Entity linker service");

        private final String name;

        Service(String name)
        {
            this.name = name;
        }

        @Override
        public String toString()
        {
            return this.name;
        }
    }

    private final static String LOG_FILE_NAME = "log.txt";

    public static void log(Service service, String log)
    {
        String logFile = Configuration.getLogDir() + "/" + LOG_FILE_NAME;

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(logFile, true)))
        {
            Date date = new Date();
            writer.append(service.toString());
            writer.append(" (");
            writer.append(date.toString());
            writer.append("): ");
            writer.append(log);
            writer.newLine();
            writer.flush();
        }

        catch (IOException e)
        {
            Logger.log(Logger.Level.ERROR, "Failed logging to logging file: " + e.getMessage());
        }
    }
}
