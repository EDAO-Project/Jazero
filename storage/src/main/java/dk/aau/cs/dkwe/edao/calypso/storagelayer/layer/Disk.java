package dk.aau.cs.dkwe.edao.calypso.storagelayer.layer;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class Disk implements Storage<File>
{
    private File dir;

    public Disk(File storageDirectory)
    {
        this.dir = storageDirectory;

        if (!this.dir.exists() && !this.dir.mkdirs())
        {
            throw new RuntimeException("Could not create directory '" + this.dir.toString() + "' to store on disk");
        }

        else if (!this.dir.isDirectory())
        {
            throw new IllegalArgumentException("Argument is not a directory: '" + this.dir.toString() + "'");
        }
    }

    public File getDirectory()
    {
        return this.dir.getAbsoluteFile();
    }

    @Override
    public boolean insert(File element)
    {
        Path sourceFile = Path.of(element.getAbsolutePath()),
                targetFile = Path.of(this.dir.toString() + "/" + element.getName());

        try
        {
            Files.copy(sourceFile, targetFile);
            return true;
        }

        catch (IOException e)
        {
            return false;
        }
    }

    @Override
    public int count()
    {
        return this.dir.list().length;
    }

    @Override
    public Iterator<File> iterator()
    {
        return List.of(Objects.requireNonNull(this.dir.listFiles())).iterator();
    }

    @Override
    public Set<File> elements()
    {
        Set<String> filesStr = Set.of(Objects.requireNonNull(this.dir.list()));
        return filesStr.stream()
                .map(s -> new File(this.dir.toString() + "/" + s))
                .collect(Collectors.toSet());
    }

    @Override
    public Set<File> elements(Predicate<File> predicate)
    {
        return Set.of(Objects.requireNonNull(this.dir.list())).stream()
                .map(s -> new File(this.dir.toString() + "/" + s))
                .filter(predicate)
                .collect(Collectors.toSet());
    }
}
