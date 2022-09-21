package dk.aau.cs.dkwe.edao.calypso.datalake.utilities;

import dk.aau.cs.dkwe.edao.calypso.datalake.structures.table.Table;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class UtilsTest
{
    private List<Double> c1 = List.of(1.0, 2.0, 3.0), c2 = List.of(5.0, 10.0, 15.0);

    @Test
    public void testAverageVector()
    {
        Table.Row<List<Double>> row = new Table.Row<>(this.c1, this.c2);
        List<Double> avgAvector = Utils.getAverageVector(row);
        assertEquals(2.0, avgAvector.get(0), 0);
        assertEquals(10.0, avgAvector.get(1), 0);
    }

    @Test
    public void testMaxVector()
    {
        Table.Row<List<Double>> row = new Table.Row<>(this.c1, this.c2);
        List<Double> maxVector = Utils.getMaxPerColumnVector(row);
        assertEquals(5.0, maxVector.get(0), 0);
        assertEquals(10.0, maxVector.get(1), 0);
        assertEquals(15.0, maxVector.get(2), 0);
    }
}
