package dk.aau.cs.dkwe.edao.calypso.knowledgegraph.middleware;

import java.io.File;
import java.io.IOException;

public abstract class Neo4JHandler
{
    protected static final String BASE = "neo4j/";
    protected static final String HOME = BASE + "neo4j-server/";
    protected static final String KG_DIR = Neo4JHandler.BASE + "kg";
    private static final String INSTALL = BASE + "install.sh";

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
}
