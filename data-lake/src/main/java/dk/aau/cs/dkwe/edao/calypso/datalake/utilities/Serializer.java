package dk.aau.cs.dkwe.edao.calypso.datalake.utilities;

public abstract class Serializer
{
    protected abstract String abstractSerialize();

    public String serialize()
    {
        return abstractSerialize();
    }
}
