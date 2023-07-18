package dk.aau.cs.dkwe.edao.calypso.datalake.store;

import dk.aau.cs.dkwe.edao.calypso.datalake.structures.Id;
import dk.aau.cs.dkwe.edao.calypso.datalake.structures.IdDictionary;

import java.io.Serializable;
import java.util.*;

/**
 * Mapping from table cell to KG entity URI
 * A trie is maybe better, where leafs contain IDs and no duplicate bidirectional mapping.
 */
public class EntityLinking implements Linker<String, String>, Serializable
{
    private IdDictionary<String> uriDict, tableCellDict;
    private final Map<Id, Id> tableCellToUri;    // Table cell text to entity URI
    private final Map<Id, Id> uriToTableCell;    // entity URI to table cell text
    String tableEntityPrefix, kgEntityPrefix;

    public EntityLinking(String cellPrefix, String uriPrefix)
    {
        this.uriDict = new IdDictionary<>(false);
        this.tableCellDict = new IdDictionary<>(false);
        this.tableCellToUri = new HashMap<>();
        this.uriToTableCell = new HashMap<>();
        this.tableEntityPrefix = cellPrefix;
        this.kgEntityPrefix = uriPrefix;
    }

    public EntityLinking(IdDictionary<String> uriDict, IdDictionary<String> cellDict, String cellPrefix, String uriPrefix)
    {
        this(cellPrefix, uriPrefix);
        this.uriDict = uriDict;
        this.tableCellDict = cellDict;
    }

    public Id uriLookup(String uri)
    {
        return this.uriDict.get(uri.substring(this.kgEntityPrefix.length()));
    }

    public String uriLookup(Id id)
    {
        return this.kgEntityPrefix + this.uriDict.get(id);
    }

    public Id cellLookup(String cellText)
    {
        return this.tableCellDict.get(cellText.substring(this.tableEntityPrefix.length()));
    }

    public String cellLookup(Id id)
    {
        return this.tableEntityPrefix + this.tableCellDict.get(id);
    }

    public Iterator<Id> uriIds()
    {
        return this.uriDict.elements().asIterator();
    }

    public Iterator<Id> cellIds()
    {
        return this.tableCellDict.elements().asIterator();
    }

    /**
     * Mapping from table cell text to KG entity URI
     * @param  cellText Table cell text
     * @return Entity URI or null if absent
     */
    @Override
    public String mapTo(String cellText)
    {
        if (!cellText.startsWith(this.tableEntityPrefix))
            throw new IllegalArgumentException("Table cell text does not start with specified table cell prefix");

        Id cellId = this.tableCellDict.get(cellText.substring(this.tableEntityPrefix.length()));

        if (cellId == null)
            return null;

        Id uriId = this.tableCellToUri.get(cellId);

        if (uriId == null)
            return null;

        return this.kgEntityPrefix + this.uriDict.get(uriId);
    }

    /**
     * Mapping from KG entity URI to table cell text
     * @param uri of KG entity
     * @return Cell text or null if absent
     */
    @Override
    public String mapFrom(String uri)
    {
        if (!uri.startsWith(this.kgEntityPrefix))
            throw new IllegalArgumentException("Entity URI does not start with specified prefix");

        Id uriId = this.uriDict.get(uri.substring(this.kgEntityPrefix.length()));

        if (uriId == null)
            return null;

        Id cellId = this.uriToTableCell.get(uriId);

        if (cellId == null)
            return null;

        return this.tableEntityPrefix + this.tableCellDict.get(cellId);
    }

    /**
     * Adds mapping
     * @param tableCell Table cell text
     * @param uri of KG entity
     */
    @Override
    public void addMapping(String tableCell, String uri)
    {
        if (!tableCell.startsWith(this.tableEntityPrefix) || !uri.startsWith(this.kgEntityPrefix))
            throw new IllegalArgumentException("Table cell text and/or entity URI do not start with given prefix");

        String cellNoPrefix = tableCell.substring(this.tableEntityPrefix.length()),
                uriNoPrefix = uri.substring(this.kgEntityPrefix.length());
        Id cellId = this.tableCellDict.get(cellNoPrefix), uriId = this.uriDict.get(uriNoPrefix);

        if (cellId == null)
            this.tableCellDict.put(cellNoPrefix, (cellId = Id.alloc()));

        if (uriId == null)
            this.uriDict.put(uriNoPrefix, (uriId = Id.alloc()));

        this.tableCellToUri.putIfAbsent(cellId, uriId);
        this.uriToTableCell.putIfAbsent(uriId, cellId);
    }

    /**
     * Clears mappings and dictionary
     */
    @Override
    public void clear()
    {
        this.tableCellToUri.clear();
        this.uriToTableCell.clear();
        this.uriDict.clear();
        this.tableCellDict.clear();
    }

    public String getKgEntityPrefix()
    {
        return this.kgEntityPrefix;
    }

    public String getTableEntityPrefix()
    {
        return this.tableEntityPrefix;
    }
}
