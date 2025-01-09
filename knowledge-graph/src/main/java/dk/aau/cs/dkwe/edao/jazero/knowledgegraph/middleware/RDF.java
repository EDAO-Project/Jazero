package dk.aau.cs.dkwe.edao.jazero.knowledgegraph.middleware;

import java.io.File;

public abstract class RDF
{
    protected File rdfFile;
    protected FileFormat format;

    protected RDF(File file, FileFormat format)
    {
        this.rdfFile = file;
        this.format = format;
    }
}
