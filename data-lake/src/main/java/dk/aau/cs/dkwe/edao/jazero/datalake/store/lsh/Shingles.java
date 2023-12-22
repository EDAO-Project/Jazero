package dk.aau.cs.dkwe.edao.jazero.datalake.store.lsh;

import java.util.List;
import java.util.Set;

public interface Shingles
{
    Set<List<String>> shingles();
}
