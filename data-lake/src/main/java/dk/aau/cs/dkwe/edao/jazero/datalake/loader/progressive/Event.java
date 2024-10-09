package dk.aau.cs.dkwe.edao.jazero.datalake.loader.progressive;

import java.util.List;

public record Event(List<String> tableIds, Type type)
{
    public enum Type
    {
        INDEX,
        INSERTION,
        SEARCH
    }
}
