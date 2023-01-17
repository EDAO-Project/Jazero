package dk.aau.cs.dkwe.edao.calypso.datalake.store;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.springframework.test.util.AssertionErrors.assertNull;

public class EntityLinkingTest
{
    private final EntityLinking linker = new EntityLinking("wiki:", "uri:");

    @BeforeEach
    public void init()
    {
        this.linker.addMapping("wiki:wiki1", "uri:uri1");
        this.linker.addMapping("wiki:wiki2", "uri:uri2");
        this.linker.addMapping("wiki:wiki3", "uri:uri3");
    }

    @Test
    public void testGetDictionary()
    {
        Set<Integer> ids = new HashSet<>();
        ids.add(this.linker.cellLookup("wiki:wiki1").id());
        ids.add(this.linker.cellLookup("wiki:wiki2").id());
        ids.add(this.linker.cellLookup("wiki:wiki3").id());
        ids.add(this.linker.uriLookup("uri:uri1").id());
        ids.add(this.linker.uriLookup("uri:uri2").id());
        ids.add(this.linker.uriLookup("uri:uri3").id());

        assertEquals(6, ids.size());
        ids.forEach(id -> assertTrue(id >= 0));
    }

    @Test
    public void testDictionaryNotExists()
    {
        assertNull("Wiki lookup was not null", this.linker.cellLookup("wiki:wiki0"));
        assertNull("KG URI lookup was not null", this.linker.uriLookup("uri:uri0"));
    }

    @Test
    public void testAddDuplicates()
    {
        Set<Integer> ids1 = Set.of(this.linker.uriLookup("uri:uri1").id(),
                this.linker.uriLookup("uri:uri2").id(), this.linker.uriLookup("uri:uri3").id());
        assertEquals(3, ids1.size());
        this.linker.addMapping("wiki:wiki1", "uri:uri1");
        this.linker.addMapping("wiki:wiki1", "uri:uri2");
        this.linker.addMapping("wiki:wiki1", "uri:uri3");

        Set<Integer> ids2 = Set.of(this.linker.uriLookup("uri:uri1").id(),
                this.linker.uriLookup("uri:uri2").id(), this.linker.uriLookup("uri:uri3").id());
        assertEquals(3, ids2.size());
        ids2.forEach(id -> assertTrue(ids1.contains(id)));
    }

    @Test
    public void testGetWikiMapping()
    {
        assertEquals("uri:uri1", this.linker.mapTo("wiki:wiki1"));
        assertEquals("uri:uri2", this.linker.mapTo("wiki:wiki2"));
        assertEquals("uri:uri3", this.linker.mapTo("wiki:wiki3"));

        this.linker.addMapping("wiki:wiki1", "uri:uri4");
        assertEquals("uri:uri1", this.linker.mapTo("wiki:wiki1"));
    }
}
