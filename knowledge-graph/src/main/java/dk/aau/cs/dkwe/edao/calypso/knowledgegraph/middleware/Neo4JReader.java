package dk.aau.cs.dkwe.edao.calypso.knowledgegraph.middleware;

import dk.aau.cs.dkwe.edao.calypso.datalake.loader.IndexIO;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

public class Neo4JReader extends Neo4JHandler implements IndexIO
{
    private static final String KG_FILE = Neo4JHandler.KG_DIR +  "kg.ttl";

    @Override
    public void performIO() throws IOException
    {
        File kgDir = new File(Neo4JHandler.KG_DIR);

        if (!kgDir.isDirectory())
        {
            throw new IOException("Folder of KG files does not exist");
        }

        File[] kgFiles = kgDir.listFiles();

        if (kgFiles.length == 0)
        {
            throw new IOException("There are no KG files to read");
        }

        try (FileOutputStream output = new FileOutputStream(KG_FILE))
        {
            for (File kgFile : kgFiles)
            {
                FileInputStream input = new FileInputStream(kgFile);
                int c;

                while ((c = input.read()) != -1)
                {
                    output.write(c);
                }

                input.close();
            }

            output.flush();
        }

        catch (IOException e)
        {
            throw new IOException("Failed appending KG files into one");
        }
    }

    public String getGraphFile()
    {
        if (!new File(KG_FILE).exists())
        {
            throw new RuntimeException("KG file does not exist: Make sure to run performIO() first");
        }

        return KG_FILE;
    }
}
