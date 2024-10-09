package com.roblox.trino.udfs.datasketches;

import com.roblox.trino.udfs.TrinoUdfsPlugin;
import com.roblox.trino.udfs.datasketches.stringitems.StringItemsSketchProxy;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedRow;
import io.trino.testing.StandaloneQueryRunner;
import org.apache.datasketches.common.ArrayOfStringsSerDe;
import org.apache.datasketches.frequencies.ItemsSketch;
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
public class StringItemsTest
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

    private ItemsSketch<String> sketchWithValues(String[] values)
    {
        ItemsSketch<String> sketch = new ItemsSketch<>(StringItemsSketchProxy.DEFAULT_MAP_SIZE);
        for (String value : values) {
            sketch.update(value);
        }
        return sketch;
    }

    private ItemsSketch<String> sketchWithValues(String[] values, int maxMapSize)
    {
        ItemsSketch<String> sketch = new ItemsSketch<>(maxMapSize);
        for (String value : values) {
            sketch.update(value);
        }
        return sketch;
    }

    private String sketchToValueString(ItemsSketch<String> sketch)
    {
        byte[] serialized = sketch.toByteArray(new ArrayOfStringsSerDe());
        StringBuilder out = new StringBuilder();

        for (byte b : serialized) {
            out.append(String.format("%02X", b));
        }

        return "(CAST(X'" + out + "' AS VARBINARY))";
    }

    @Test
    public void testOnSmallValues()
    {
        String valuesString = arrayToValuesString(new String[] {"'a'", "'b'", "'a'", "'a'", "'c'", "'b'"});

        MaterializedResult output = runner.execute("SELECT string_items_sketch_estimate(string_items_sketch(x), 'a') FROM " + valuesString + " AS W(x)");
        List<MaterializedRow> result = output.getMaterializedRows();
        long value = (long) result.get(0).getFields().get(0);

        Assertions.assertEquals(3, value);
    }

    @Test
    public void testOnSmallValuesWithSize()
    {
        String valuesString = arrayToValuesString(new String[] {"'a'", "'b'", "'a'", "'a'", "'c'", "'b'"});

        MaterializedResult output = runner.execute("SELECT string_items_sketch_estimate(string_items_sketch(x, 128), 'a') FROM " + valuesString + " AS W(x)");
        List<MaterializedRow> result = output.getMaterializedRows();
        long value = (long) result.get(0).getFields().get(0);

        Assertions.assertEquals(3, value);
    }

    @Test
    public void testOnSmallValuesArray()
    {
        String valuesString = arrayToValuesString(new String[] {"'a'", "'b'", "'a'", "'a'", "'c'", "'b'"});

        MaterializedResult output = runner.execute("SELECT string_items_sketch_estimate(string_items_sketch(x), ARRAY['a', 'b', 'c']) FROM " + valuesString + " AS W(x)");
        List<MaterializedRow> result = output.getMaterializedRows();
        ArrayList<Long> values = (ArrayList<Long>) result.get(0).getFields().get(0);
        ArrayList<Long> expected = new ArrayList<Long>(List.of(3L, 2L, 1L));

        Assertions.assertEquals(expected, values);
    }

    @Test
    public void testOnLargeValues()
    {
        String query = "WITH data AS (SELECT string_items_sketch(name) AS sketch FROM tpch.sf1.nation) " +
                "SELECT string_items_sketch_estimate_lb(sketch, 'UNITED KINGDOM'), string_items_sketch_estimate_ub(sketch, 'UNITED KINGDOM') FROM data";
        MaterializedResult output = runner.execute(query);
        List<MaterializedRow> result = output.getMaterializedRows();
        long lower = (long) result.get(0).getFields().get(0);
        long upper = (long) result.get(0).getFields().get(1);
        long expected = 1;
        Assertions.assertTrue(expected >= lower && expected <= upper);
    }

    @Test
    public void testOnSketches()
    {
        ItemsSketch<String> sketch1 = sketchWithValues(new String[] {"a", "b", "b", "c", "c"});
        ItemsSketch<String> sketch2 = sketchWithValues(new String[] {"c", "d", "d", "e"});
        ItemsSketch<String> sketch3 = sketchWithValues(new String[] {"f", "g", "g", "g", "g", "g"});

        String valuesString = arrayToValuesString(new String[] {
                sketchToValueString(sketch1),
                sketchToValueString(sketch2),
                sketchToValueString(sketch3)
        });

        String query = "SELECT string_items_sketch_estimate(string_items_sketch(x), 'g') FROM " + valuesString + " AS W(x)";
        MaterializedResult output = runner.execute(query);
        List<MaterializedRow> result = output.getMaterializedRows();
        long value = (long) result.get(0).getFields().get(0);

        Assertions.assertEquals(5, value);
    }

    @Test
    public void testFrequentItems()
    {
        ItemsSketch<String> sketch1 = sketchWithValues(new String[] {"a", "b", "b", "c", "c"});
        ItemsSketch<String> sketch2 = sketchWithValues(new String[] {"c", "d", "d", "e"});
        ItemsSketch<String> sketch3 = sketchWithValues(new String[] {"f", "g", "g", "g", "g", "g"});

        String valuesString = arrayToValuesString(new String[] {
                sketchToValueString(sketch1),
                sketchToValueString(sketch2),
                sketchToValueString(sketch3)
        });

        String query = "SELECT string_items_sketch_frequent_items(string_items_sketch(x), false) FROM " + valuesString + " AS W(x)";
        MaterializedResult output = runner.execute(query);
        List<MaterializedRow> result = output.getMaterializedRows();
        ArrayList<String> value = (ArrayList<String>) result.get(0).getFields().get(0);
        ArrayList<String> expected = new ArrayList<>(List.of("g", "c", "d", "b", "a", "f", "e"));

        Assertions.assertEquals(expected, value);
    }

    @Test
    public void testFrequentItemsFalseNegatives()
    {
        ItemsSketch<String> sketch1 = sketchWithValues(new String[] {"a", "b", "b", "c", "c"});
        ItemsSketch<String> sketch2 = sketchWithValues(new String[] {"c", "d", "d", "e"});
        ItemsSketch<String> sketch3 = sketchWithValues(new String[] {"f", "g", "g", "g", "g", "g"});

        String valuesString = arrayToValuesString(new String[] {
                sketchToValueString(sketch1),
                sketchToValueString(sketch2),
                sketchToValueString(sketch3)
        });

        String query = "SELECT string_items_sketch_frequent_items(string_items_sketch(x), true) FROM " + valuesString + " AS W(x)";
        MaterializedResult output = runner.execute(query);
        List<MaterializedRow> result = output.getMaterializedRows();
        ArrayList<String> value = (ArrayList<String>) result.get(0).getFields().get(0);
        ArrayList<String> expected = new ArrayList<>(List.of("g", "c", "d", "b", "a", "f", "e"));

        Assertions.assertEquals(expected, value);
    }

    @Test
    public void testGroupBy()
    {
        ItemsSketch<String> sketch1 = sketchWithValues(new String[] {"f", "g", "g", "g", "g", "g"});
        ItemsSketch<String> sketch2 = sketchWithValues(new String[] {"f", "g", "g", "g", "g", "g"});
        ItemsSketch<String> sketch3 = sketchWithValues(new String[] {"f", "g", "g", "g", "g", "g"});
        String valuesString = arrayToValuesString(new String[] {
                sketchToValueString(sketch1),
                sketchToValueString(sketch2),
                sketchToValueString(sketch3)
        });

        String query = "SELECT string_items_sketch_estimate(x, 'g') FROM " + valuesString + " AS W(x) GROUP BY x";
        MaterializedResult output = runner.execute(query);
        List<MaterializedRow> result = output.getMaterializedRows();
        long value = (long) result.get(0).getFields().get(0);

        Assertions.assertEquals(5, value);
    }
}
