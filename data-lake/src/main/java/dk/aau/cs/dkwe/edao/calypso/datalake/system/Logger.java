package dk.aau.cs.dkwe.edao.calypso.datalake.system;

import org.slf4j.LoggerFactory;

import java.util.Date;

public class Logger
{
    public enum Level
    {
        INFO(1), DEBUG(2), RESULT(3), ERROR(4);

        private int level;

        Level(int level)
        {
            this.level = level;
        }

        public String toString()
        {
            switch (this.level)
            {
                case 1:
                    return INFO.name();

                case 2:
                    return ERROR.name();

                case 3:
                    return RESULT.name();

                case 4:
                    return DEBUG.name();

                default:
                    return null;
            }
        }

        public int getLevel()
        {
            return this.level;
        }

        public static Level parse(String str)
        {
            if ("info".equals(str.toLowerCase()))
                return INFO;

            else if ("error".equals(str.toLowerCase()))
                return ERROR;

            else if ("debug".equals(str.toLowerCase()))
                return DEBUG;

            else if ("result".equals(str.toLowerCase()))
                return RESULT;

            return null;
        }
    }

    private static int prevLength = 0;
    private static boolean prevWasNewLine = false;

    public static void log(Level level, String message)
    {
        Level configuredLevel = Level.parse(Configuration.getLogLevel());

        if (configuredLevel != null && level.getLevel() >= configuredLevel.getLevel())
        {
            System.out.print("\n(" + new Date() + ") - " + level + ": " + message);
            prevWasNewLine = true;
        }
    }
}
