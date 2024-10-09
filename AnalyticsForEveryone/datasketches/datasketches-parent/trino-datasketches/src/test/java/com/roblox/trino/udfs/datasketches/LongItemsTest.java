package com.roblox.trino.udfs.datasketches;

import com.roblox.trino.udfs.TrinoUdfsPlugin;
import com.roblox.trino.udfs.datasketches.stringitems.StringItemsSketchProxy;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedRow;
import io.trino.testing.StandaloneQueryRunner;
import org.apache.datasketches.frequencies.LongsSketch;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.util.ArrayList;
import java.util.List;

import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class LongItemsTest
{
    private StandaloneQueryRunner runner;

    @BeforeAll
    public void init()
    {
        runner = new StandaloneQueryRunner(testSessionBuilder().build());
        runner.installPlugin(new TrinoUdfsPlugin());
        runner.installPlugin(new TpchPlugin());
        runner.createCatalog("tpch", "tpch");
    }

    @AfterAll
    public void teardown()
    {
        runner.close();
        runner = null;
    }

    private String arrayToValuesString(Object[] values)
    {
        StringBuilder out = new StringBuilder();
        out.append("(VALUES ");
        for (int i = 0; i < values.length; i++) {
            out.append("(").append(values[i]).append(")");
            if (i < values.length - 1) {
                out.append(", ");
            }
        }
        out.append(")");
        return out.toString();
    }

    private String longArrayToValuesString(long[] values)
    {
        StringBuilder out = new StringBuilder();
        out.append("(VALUES ");
        for (int i = 0; i < values.length; i++) {
            out.append("(").append(values[i]).append(")");
            if (i < values.length - 1) {
                out.append(", ");
            }
        }
        out.append(")");
        return out.toString();
    }

    private LongsSketch sketchWithValues(long[] values)
    {
        LongsSketch sketch = new LongsSketch(StringItemsSketchProxy.DEFAULT_MAP_SIZE);
        for (long value : values) {
            sketch.update(value);
        }
        return sketch;
    }

    private LongsSketch sketchWithValues(long[] values, int maxMapSize)
    {
        LongsSketch sketch = new LongsSketch(maxMapSize);
        for (long value : values) {
            sketch.update(value);
        }
        return sketch;
    }

    private String sketchToValueString(LongsSketch sketch)
    {
        byte[] serialized = sketch.toByteArray();
        StringBuilder out = new StringBuilder();

        for (byte b : serialized) {
            out.append(String.format("%02X", b));
        }

        return "(CAST(X'" + out + "' AS VARBINARY))";
    }

    @Test
    public void testOnSmallValues()
    {
        String valuesString = longArrayToValuesString(new long[] {1, 2, 2, 2, 3, 4});

        MaterializedResult output = runner.execute("SELECT long_items_sketch_estimate(long_items_sketch(x), 2) FROM " + valuesString + " AS W(x)");
        List<MaterializedRow> result = output.getMaterializedRows();
        long value = (long) result.get(0).getFields().get(0);

        Assertions.assertEquals(3, value);
    }

    @Test
    public void testOnSmallValuesWithSize()
    {
        String valuesString = longArrayToValuesString(new long[] {1, 2, 2, 2, 3, 4});

        MaterializedResult output = runner.execute("SELECT long_items_sketch_estimate(long_items_sketch(x, 128), 2) FROM " + valuesString + " AS W(x)");
        List<MaterializedRow> result = output.getMaterializedRows();
        long value = (long) result.get(0).getFields().get(0);

        Assertions.assertEquals(3, value);
    }

    @Test
    public void testOnSmallValuesArray()
    {
        String valuesString = longArrayToValuesString(new long[] {1, 2, 2, 2, 3, 3});

        MaterializedResult output = runner.execute("SELECT long_items_sketch_estimate(long_items_sketch(x), ARRAY[1, 2, 3]) FROM " + valuesString + " AS W(x)");
        List<MaterializedRow> result = output.getMaterializedRows();
        ArrayList<Long> values = (ArrayList<Long>) result.get(0).getFields().get(0);
        ArrayList<Long> expected = new ArrayList<Long>(List.of(1L, 3L, 2L));

        Assertions.assertEquals(expected, values);
    }

    @Test
    public void testOnLargeValues()
    {
        String query = "WITH data AS (SELECT long_items_sketch(shippriority) AS sketch FROM tpch.sf1.orders) " +
                "SELECT long_items_sketch_estimate_lb(sketch, 0), long_items_sketch_estimate_ub(sketch, 0) FROM data";
        MaterializedResult output = runner.execute(query);
        List<MaterializedRow> result = output.getMaterializedRows();
        long lower = (long) result.get(0).getFields().get(0);
        long upper = (long) result.get(0).getFields().get(1);
        long expected = 1500000;
        Assertions.assertTrue(expected >= lower && expected <= upper);
    }

    @Test
    public void testOnSketches()
    {
        LongsSketch sketch1 = sketchWithValues(new long[] {1, 2, 2, 2, 3, 4});
        LongsSketch sketch2 = sketchWithValues(new long[] {3, 4, 4, 5, 6, 6});
        LongsSketch sketch3 = sketchWithValues(new long[] {7, 8, 8, 8, 8, 8});

        String valuesString = arrayToValuesString(new String[] {
                sketchToValueString(sketch1),
                sketchToValueString(sketch2),
                sketchToValueString(sketch3)
        });

        String query = "SELECT long_items_sketch_estimate(long_items_sketch(x), 8) FROM " + valuesString + " AS W(x)";
        MaterializedResult output = runner.execute(query);
        List<MaterializedRow> result = output.getMaterializedRows();
        long value = (long) result.get(0).getFields().get(0);

        Assertions.assertEquals(5, value);
    }

    @Test
    public void testFrequentItems()
    {
        LongsSketch sketch1 = sketchWithValues(new long[] {1, 2, 2, 2, 3, 4});
        LongsSketch sketch2 = sketchWithValues(new long[] {3, 4, 4, 5, 6, 6});
        LongsSketch sketch3 = sketchWithValues(new long[] {7, 8, 8, 8, 8, 8});

        String valuesString = arrayToValuesString(new String[] {
                sketchToValueString(sketch1),
                sketchToValueString(sketch2),
                sketchToValueString(sketch3)
        });

        String query = "SELECT long_items_sketch_frequent_items(long_items_sketch(x), false) FROM " + valuesString + " AS W(x)";
        MaterializedResult output = runner.execute(query);
        List<MaterializedRow> result = output.getMaterializedRows();
        ArrayList<Long> value = (ArrayList<Long>) result.get(0).getFields().get(0);
        ArrayList<Long> expected = new ArrayList<>(List.of(8L, 4L, 2L, 6L, 3L, 7L, 1L, 5L));

        Assertions.assertEquals(expected, value);
    }

    @Test
    public void testFrequentItemsFalseNegatives()
    {
        LongsSketch sketch1 = sketchWithValues(new long[] {1, 2, 2, 2, 3, 4});
        LongsSketch sketch2 = sketchWithValues(new long[] {3, 4, 4, 5, 6, 6});
        LongsSketch sketch3 = sketchWithValues(new long[] {7, 8, 8, 8, 8, 8});

        String valuesString = arrayToValuesString(new String[] {
                sketchToValueString(sketch1),
                sketchToValueString(sketch2),
                sketchToValueString(sketch3)
        });

        String query = "SELECT long_items_sketch_frequent_items(long_items_sketch(x), true) FROM " + valuesString + " AS W(x)";
        MaterializedResult output = runner.execute(query);
        List<MaterializedRow> result = output.getMaterializedRows();
        ArrayList<Long> value = (ArrayList<Long>) result.get(0).getFields().get(0);
        ArrayList<Long> expected = new ArrayList<>(List.of(8L, 4L, 2L, 6L, 3L, 7L, 1L, 5L));

        Assertions.assertEquals(expected, value);
    }

    @Test
    public void testGroupBy()
    {
        LongsSketch sketch1 = sketchWithValues(new long[] {7, 8, 8, 8, 8, 8});
        LongsSketch sketch2 = sketchWithValues(new long[] {7, 8, 8, 8, 8, 8});
        LongsSketch sketch3 = sketchWithValues(new long[] {7, 8, 8, 8, 8, 8});
        String valuesString = arrayToValuesString(new String[] {
                sketchToValueString(sketch1),
                sketchToValueString(sketch2),
                sketchToValueString(sketch3)
        });

        String query = "SELECT long_items_sketch_estimate(x, 8) FROM " + valuesString + " AS W(x) GROUP BY x";
        MaterializedResult output = runner.execute(query);
        List<MaterializedRow> result = output.getMaterializedRows();
        long value = (long) result.get(0).getFields().get(0);

        Assertions.assertEquals(5, value);
    }
}