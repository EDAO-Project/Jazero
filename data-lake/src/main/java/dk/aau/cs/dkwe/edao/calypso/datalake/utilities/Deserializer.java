package dk.aau.cs.dkwe.edao.calypso.datalake.utilities;

public abstract class Deserializer<E>
{
    protected abstract E abstractDeserialize();

    public E deserialize()
    {
        return abstractDeserialize();
    }
}
