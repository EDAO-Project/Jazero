package dk.aau.cs.dkwe.edao.jazero.datalake.system;

import java.io.File;
import java.io.IOException;

public final class FileUtil
{
    private static int runCommand(String command)
    {
        try
        {
            Runtime rt = Runtime.getRuntime();
            Process proc = rt.exec(command);

            return proc.waitFor();
        }

        catch (IOException | InterruptedException e)
        {
            return -1;
        }
    }

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

        String command = "cp" + (src.isDirectory() ? " -r " : " ") + src.getAbsolutePath() + " " + tar.getAbsolutePath();
        return runCommand(command);
    }

    public static int move(File src, File tar)
    {
        if (!tar.isDirectory())
        {
            return -1;
        }

        String command = "mv " + src.getAbsolutePath() + " " + tar.getAbsolutePath();
        return runCommand(command);
    }

    /**
     * Forcibly removes file or directory
     * @param f File or directory to remove
     * @return Exit code
     */
    public static int remove(File f)
    {
        if (!f.exists())
        {
            return -1;
        }

        String command = "rm -rf " + f.getAbsolutePath();
        return runCommand(command);
    }
}
