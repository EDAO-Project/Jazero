package dk.aau.cs.dkwe.edao.calypso.knowledgegraph.middleware;

public enum FileFormat
{
    NT(".nt"),
    TTL(".ttl");

    private final String suffix;

    FileFormat(String suffix)
    {
        this.suffix = suffix;
    }

    public String getSuffix()
    {
        return this.suffix;
    }
}
