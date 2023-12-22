package dk.aau.cs.dkwe.edao.jazero.datalake.system;

import java.util.Date;

public class Logger
{
    public enum Level
    {
        INFO(1), DEBUG(2), RESULT(3), ERROR(4);

        private final int level;

        Level(int level)
        {
            this.level = level;
        }

        public String toString()
        {
            return switch (this.level) {
                case 1 -> INFO.name();
                case 2 -> ERROR.name();
                case 3 -> RESULT.name();
                case 4 -> DEBUG.name();
                default -> null;
            };
        }

        public int getLevel()
        {
            return this.level;
        }

        public static Level parse(String str)
        {
            return switch (str.toLowerCase()) {
                case "info" -> INFO;
                case "error" -> ERROR;
                case "debug" -> DEBUG;
                case "result" -> RESULT;
                default -> null;
            };

        }
    }

    public static void log(Level level, String message)
    {
        Level configuredLevel = Level.parse(Configuration.getLogLevel());

        if (configuredLevel != null && level.getLevel() >= configuredLevel.getLevel())
        {
            System.out.print("(" + new Date() + ") - " + level + ": " + message + "\n");
        }
    }
}
