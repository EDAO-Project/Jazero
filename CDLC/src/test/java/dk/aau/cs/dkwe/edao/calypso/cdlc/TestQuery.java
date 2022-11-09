package dk.aau.cs.dkwe.edao.calypso.cdlc;

import dk.aau.cs.dkwe.edao.calypso.cdlc.query.Query;
import dk.aau.cs.dkwe.edao.calypso.cdlc.query.QueryFactory;
import dk.aau.cs.dkwe.edao.calypso.datalake.parser.TableParser;
import dk.aau.cs.dkwe.edao.calypso.datalake.structures.table.Table;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

public class TestQuery
{
    private static final Table<String> base1Tuple = TableParser.toTable(List.of(List.of("c1", "c2", "c3"))),
            base2Tuple = TableParser.toTable(List.of(List.of("c1", "c2", "c3"), List.of("c4", "c5", "c5"))),
            base3Tuple = TableParser.toTable(List.of(List.of("c1", "c2", "c3"), List.of("c4", "c5", "c5"), List.of("c6", "c7", "c8")));

    @Test
    public void test1Tuple()
    {
        List<List<String>> copy = List.of(List.of("c1", "c2", "c3"));
        Query q = QueryFactory.create(copy);
        assertEquals(q, QueryFactory.create(base1Tuple));
        assertNotEquals(q, QueryFactory.create(base2Tuple));
        assertNotEquals(q, QueryFactory.create(base3Tuple));
    }

    @Test
    public void test2Tuples()
    {
        List<List<String>> copy = List.of(List.of("c1", "c2", "c3"), List.of("c4", "c5", "c5"));
        Query q = QueryFactory.create(copy);
        assertNotEquals(q, QueryFactory.create(base1Tuple));
        assertEquals(q, QueryFactory.create(base2Tuple));
        assertNotEquals(q, QueryFactory.create(base3Tuple));
    }

    @Test
    public void test3Tuples()
    {
        List<List<String>> copy = List.of(List.of("c1", "c2", "c3"), List.of("c4", "c5", "c5"), List.of("c6", "c7", "c8"));
        Query q = QueryFactory.create(copy);
        assertNotEquals(q, QueryFactory.create(base1Tuple));
        assertNotEquals(q, QueryFactory.create(base2Tuple));
        assertEquals(q, QueryFactory.create(base3Tuple));
    }

    @Test
    public void testToString1()
    {
        Query q = QueryFactory.create(base1Tuple);
        assertEquals("c1<>c2<>c3", q.toString());
    }

    @Test
    public void testToString2()
    {
        Query q = QueryFactory.create(base2Tuple);
        assertEquals("c1<>c2<>c3#c4<>c5<>c5", q.toString());
    }

    @Test
    public void testToString3()
    {
        Query q = QueryFactory.create(base3Tuple);
        assertEquals("c1<>c2<>c3#c4<>c5<>c5#c6<>c7<>c8", q.toString());
    }
}
