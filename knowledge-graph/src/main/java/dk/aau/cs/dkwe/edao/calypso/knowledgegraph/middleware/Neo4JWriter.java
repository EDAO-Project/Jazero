package dk.aau.cs.dkwe.edao.calypso.knowledgegraph.middleware;

import dk.aau.cs.dkwe.edao.calypso.datalake.loader.IndexIO;
import dk.aau.cs.dkwe.edao.calypso.datalake.system.FileUtil;
import dk.aau.cs.dkwe.edao.calypso.knowledgegraph.connector.Neo4jEndpoint;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;

// TODO: Remember to handle insertion of table links

/**
 * Saves a knowledge graph in a running instance of Neo4J
 */
public class Neo4JWriter extends Neo4JHandler implements IndexIO
{
    private String file;
    private static final String IMPORT_SCRIPT = Neo4JHandler.BASE + "import.sh";
    private static final String INSERT_LINKS_SCRIPT = Neo4JHandler.BASE + "insert-links.sh";

    public Neo4JWriter(String file)
    {
        this.file = file;
    }

    /**
     * Entry method to populate Neo4J instance with a given knowledge graph from a Turtle file
     * @throws IOException When Neo4J is not installed,
     *                     necessary script files or KG file do not exist,
     *                     or when an error occurs when running the scripts
     */
    @Override
    public void performIO() throws IOException
    {
        if (!Neo4JHandler.isInstalled())
        {
            throw new IOException("Neo4J has not been installed");
        }

        else if (!new File(IMPORT_SCRIPT).exists())
        {
            throw new IOException("Data import script is missing");
        }

        else if (!new File(this.file).exists())
        {
            throw new IOException("Input KG file does not exist");
        }

        try
        {
            Runtime rt = Runtime.getRuntime();
            Process processImport = rt.exec("./" + IMPORT_SCRIPT + " " + this.file + " " + Neo4JHandler.HOME);

            if (processImport.waitFor() != 0)
            {
                throw new IOException("Data import process did not complete");
            }

            int exitCode;
            Process processCopy = rt.exec("mkdir -p " + Neo4JHandler.KG_DIR +
                    " && cp " + this.file + " " + Neo4JHandler.KG_DIR);

            if ((exitCode = processCopy.waitFor()) != 0)
            {
                throw new IOException("Saving KG file failed: exit code " + exitCode);
            }
        }

        catch (InterruptedException e)
        {
            throw new IOException("Data import process was interrupted");
        }
    }

    /**
     * Inserts table links to entities in Neo4J KG
     * @param linksFolder Folder of links Turtle files
     * @throws IOException When necessary script files of KG file do not exist,
     *                     necessary script files or folder do not exist,
     *                     or when an error occurs when running the scripts
     */
    public static void insertTableToEntities(String linksFolder) throws IOException
    {
        File links = new File(linksFolder);

        if (!Neo4JHandler.isInstalled())
        {
            throw new IOException("Neo4J has not been installed");
        }

        else if (!new File(INSERT_LINKS_SCRIPT).exists())
        {
            throw new IOException("Missing script to insert table links");
        }

        else if (links.isDirectory())
        {
            throw new IOException("Folder of table links could not be found");
        }

        try
        {
            File kgDir = new File(Neo4JHandler.KG_DIR);
            kgDir.mkdirs();

            int exitCode;
            Runtime rt = Runtime.getRuntime();
            Process process = rt.exec("./" + INSERT_LINKS_SCRIPT + " " + Neo4JHandler.HOME + " " + linksFolder);

            if ((exitCode = process.waitFor()) != 0)
            {
                throw new IOException("Table links insertion did not complete: exit code " + exitCode);
            }

            FileUtils.moveDirectory(links, kgDir);
        }

        catch (InterruptedException e)
        {
            throw new IOException("Table links insertion was interrupted");
        }
    }
}
