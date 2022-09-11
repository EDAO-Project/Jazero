package dk.aau.cs.dkwe.edao.calypso.datalake.system;

import java.io.File;
import java.io.IOException;

public final class FileUtil
{
    /**
     * Copy file or directory to target directory
     * @param src Source directory or file
     * @param tar Target directory
     * @return Exit code
     */
    public static int copy(File src, File tar)
    {
        if (!tar.isDirectory())
        {
            return -1;
        }

        try
        {
            String command = "cp" + (src.isDirectory() ? " -r " : " ") + src.getAbsolutePath() + " " + tar.getAbsolutePath();
            Runtime rt = Runtime.getRuntime();
            Process proc = rt.exec(command);

            return proc.waitFor();
        }

        catch (IOException | InterruptedException e)
        {
            return -1;
        }
    }

    public static int move(File src, File tar)
    {
        if (!tar.isDirectory())
        {
            return -1;
        }

        try
        {
            String command = "mv " + src.getAbsolutePath() + " " + tar.getAbsolutePath();
            Runtime rt = Runtime.getRuntime();
            Process proc = rt.exec(command);

            return proc.waitFor();
        }

        catch (IOException | InterruptedException e)
        {
            return -1;
        }
    }
}
