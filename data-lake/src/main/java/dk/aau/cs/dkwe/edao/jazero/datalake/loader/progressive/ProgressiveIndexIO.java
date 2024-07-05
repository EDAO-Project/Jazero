package dk.aau.cs.dkwe.edao.jazero.datalake.loader.progressive;

import dk.aau.cs.dkwe.edao.jazero.datalake.loader.IndexIO;

import java.nio.file.Path;

public interface ProgressiveIndexIO extends IndexIO
{
    boolean addTable(Path tablePath);
    void waitForCompletion();
    void pauseIndexing();
    void continueIndexing();
}
