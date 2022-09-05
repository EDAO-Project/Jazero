package dk.aau.cs.dkwe.edao.calypso.knowledgegraph.middleware;

import java.io.File;
import java.io.IOException;

public abstract class Neo4JHandler
{
    protected static final String BASE = "neo4j/";
    protected static final String HOME = BASE + "neo4j-server/";
    protected static final String HOME_IMPORT = HOME + "import/";
    protected static final String KG_DIR = BASE + "kg/";
    protected static final String CONFIG_FILE = BASE + "config.properties";
    private static final String INSTALL = BASE + "install.sh";
    private static final String START = BASE + "start.sh";
    private static final String STOP = BASE + "stop.sh";
    private static final String RESTART = BASE + "restart.sh";

    public static boolean isInstalled()
    {
        File neo4jHome = new File(HOME);
        return neo4jHome.exists() && neo4jHome.isDirectory();
    }

    public static boolean install()
    {
        File installFile = new File(INSTALL);

        if (!installFile.exists())
        {
            return false;
        }

        try
        {
            Runtime rt = Runtime.getRuntime();
            Process process = rt.exec("./" + INSTALL + " " + BASE);
            return process.waitFor() == 0;
        }

        catch (IOException | InterruptedException e)
        {
           return false;
        }
    }

    private static boolean runScript(String script, String ... args)
    {
        if (!isInstalled())
        {
            return false;
        }

        StringBuilder builder = new StringBuilder();

        for (String arg : args)
        {
            builder.append(arg).append(" ");
        }

        try
        {
            Runtime rt = Runtime.getRuntime();
            Process command = rt.exec(script + " " + builder.deleteCharAt(builder.lastIndexOf(" ")));
            return command.waitFor() == 0;
        }

        catch (IOException | InterruptedException e)
        {
            return false;
        }
    }

    public static boolean start()
    {
        return runScript(START, HOME);
    }

    public static boolean stop()
    {
        return runScript(STOP, HOME);
    }

    public static boolean restart()
    {
        return runScript(RESTART, HOME);
    }
}
