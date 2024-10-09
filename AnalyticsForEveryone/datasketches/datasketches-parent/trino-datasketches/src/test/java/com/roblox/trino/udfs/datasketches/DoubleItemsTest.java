package com.roblox.trino.udfs.datasketches;

import com.roblox.trino.udfs.TrinoUdfsPlugin;
import com.roblox.trino.udfs.datasketches.stringitems.StringItemsSketchProxy;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedRow;
import io.trino.testing.StandaloneQueryRunner;
import org.apache.datasketches.common.ArrayOfDoublesSerDe;
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
public class DoubleItemsTest
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

    private String doubleArrayToValuesString(double[] values)
    {
        StringBuilder out = new StringBuilder();
        out.append("(VALUES ");
        for (int i = 0; i < values.length; i++) {
            out.append("(DOUBLE '").append(values[i]).append("')");
            if (i < values.length - 1) {
                out.append(", ");
            }
        }
        out.append(")");
        return out.toString();
    }

    private ItemsSketch<Double> sketchWithValues(double[] values)
    {
        ItemsSketch<Double> sketch = new ItemsSketch<>(StringItemsSketchProxy.DEFAULT_MAP_SIZE);
        for (double value : values) {
            sketch.update(value);
        }
        return sketch;
    }

    private ItemsSketch<Double> sketchWithValues(double[] values, int maxMapSize)
    {
        ItemsSketch<Double> sketch = new ItemsSketch<>(maxMapSize);
        for (double value : values) {
            sketch.update(value);
        }
        return sketch;
    }

    private String sketchToValueString(ItemsSketch<Double> sketch)
    {
        byte[] serialized = sketch.toByteArray(new ArrayOfDoublesSerDe());
        StringBuilder out = new StringBuilder();

        for (byte b : serialized) {
            out.append(String.format("%02X", b));
        }

        return "(CAST(X'" + out + "' AS VARBINARY))";
    }

    @Test
    public void testOnSmallValues()
    {
        String valuesString = doubleArrayToValuesString(new double[] {0.1, 0.2, 0.2, 0.2, 0.3, 0.4});

        MaterializedResult output = runner.execute("SELECT double_items_sketch_estimate(double_items_sketch(x), DOUBLE '0.2') FROM " + valuesString + " AS W(x)");
        List<MaterializedRow> result = output.getMaterializedRows();
        long value = (long) result.get(0).getFields().get(0);

        Assertions.assertEquals(3, value);
    }

    @Test
    public void testOnSmallRealValues()
    {
        String valuesString = "(VALUES (REAL '0.1'), (REAL '0.2'), (REAL '0.2'), (REAL '0.2'), (REAL '0.3'), (REAL '0.4'))";

        MaterializedResult output = runner.execute("SELECT double_items_sketch_estimate(double_items_sketch(x), 0.2) FROM " + valuesString + " AS W(x)");
        List<MaterializedRow> result = output.getMaterializedRows();
        long value = (long) result.get(0).getFields().get(0);

        Assertions.assertEquals(3, value);
    }

    @Test
    public void testOnSmallValuesWithSize()
    {
        String valuesString = doubleArrayToValuesString(new double[] {0.1, 0.2, 0.2, 0.2, 0.3, 0.4});

        MaterializedResult output = runner.execute("SELECT double_items_sketch_estimate(double_items_sketch(x, 128), DOUBLE '0.2') FROM " + valuesString + " AS W(x)");
        List<MaterializedRow> result = output.getMaterializedRows();
        long value = (long) result.get(0).getFields().get(0);

        Assertions.assertEquals(3, value);
    }

    @Test
    public void testOnSmallValuesArray()
    {
        String valuesString = doubleArrayToValuesString(new double[] {0.1, 0.2, 0.2, 0.2, 0.3, 0.3});

        MaterializedResult output = runner.execute("SELECT double_items_sketch_estimate(double_items_sketch(x), ARRAY[DOUBLE '0.1', DOUBLE '0.2', DOUBLE '0.3']) FROM " + valuesString + " AS W(x)");
        List<MaterializedRow> result = output.getMaterializedRows();
        ArrayList<Long> values = (ArrayList<Long>) result.get(0).getFields().get(0);
        ArrayList<Long> expected = new ArrayList<Long>(List.of(1L, 3L, 2L));

        Assertions.assertEquals(expected, values);
    }

    @Test
    public void testOnLargeValues()
    {
        String query = "WITH data AS (SELECT double_items_sketch(totalprice) AS sketch FROM tpch.sf1.orders) " +
                "SELECT double_items_sketch_estimate_lb(sketch, 544089.09), double_items_sketch_estimate_ub(sketch, 544089.09) FROM data";
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
        ItemsSketch<Double> sketch1 = sketchWithValues(new double[] {0.1, 0.2, 0.2, 0.2, 0.3, 0.4});
        ItemsSketch<Double> sketch2 = sketchWithValues(new double[] {0.3, 0.4, 0.4, 0.5, 0.6, 0.6});
        ItemsSketch<Double> sketch3 = sketchWithValues(new double[] {0.7, 0.8, 0.8, 0.8, 0.8, 0.8});

        String valuesString = arrayToValuesString(new String[] {
                sketchToValueString(sketch1),
                sketchToValueString(sketch2),
                sketchToValueString(sketch3)
        });

        String query = "SELECT double_items_sketch_estimate(double_items_sketch(x), (DOUBLE '0.8')) FROM " + valuesString + " AS W(x)";
        MaterializedResult output = runner.execute(query);
        List<MaterializedRow> result = output.getMaterializedRows();
        long value = (long) result.get(0).getFields().get(0);

        Assertions.assertEquals(5, value);
    }

    @Test
    public void testFrequentItems()
    {
        ItemsSketch<Double> sketch1 = sketchWithValues(new double[] {0.1, 0.2, 0.2, 0.2, 0.3, 0.4});
        ItemsSketch<Double> sketch2 = sketchWithValues(new double[] {0.3, 0.4, 0.4, 0.5, 0.6, 0.6});
        ItemsSketch<Double> sketch3 = sketchWithValues(new double[] {0.7, 0.8, 0.8, 0.8, 0.8, 0.8});

        String valuesString = arrayToValuesString(new String[] {
                sketchToValueString(sketch1),
                sketchToValueString(sketch2),
                sketchToValueString(sketch3)
        });

        String query = "SELECT double_items_sketch_frequent_items(double_items_sketch(x), false) FROM " + valuesString + " AS W(x)";
        MaterializedResult output = runner.execute(query);
        List<MaterializedRow> result = output.getMaterializedRows();
        ArrayList<Long> value = (ArrayList<Long>) result.get(0).getFields().get(0);
        ArrayList<Double> expected = new ArrayList<>(List.of(0.8, 0.2, 0.4, 0.3, 0.6, 0.1, 0.7, 0.5));

        Assertions.assertEquals(expected, value);
    }

    @Test
    public void testFrequentItemsFalseNegatives()
    {
        ItemsSketch<Double> sketch1 = sketchWithValues(new double[] {0.1, 0.2, 0.2, 0.2, 0.3, 0.4});
        ItemsSketch<Double> sketch2 = sketchWithValues(new double[] {0.3, 0.4, 0.4, 0.5, 0.6, 0.6});
        ItemsSketch<Double> sketch3 = sketchWithValues(new double[] {0.7, 0.8, 0.8, 0.8, 0.8, 0.8});

        String valuesString = arrayToValuesString(new String[] {
                sketchToValueString(sketch1),
                sketchToValueString(sketch2),
                sketchToValueString(sketch3)
        });

        String query = "SELECT double_items_sketch_frequent_items(double_items_sketch(x), true) FROM " + valuesString + " AS W(x)";
        MaterializedResult output = runner.execute(query);
        List<MaterializedRow> result = output.getMaterializedRows();
        ArrayList<Double> value = (ArrayList<Double>) result.get(0).getFields().get(0);
        ArrayList<Double> expected = new ArrayList<>(List.of(0.8, 0.2, 0.4, 0.3, 0.6, 0.1, 0.7, 0.5));

        Assertions.assertEquals(expected, value);
    }

    @Test
    public void testGroupBy()
    {
        ItemsSketch<Double> sketch1 = sketchWithValues(new double[] {0.7, 0.8, 0.8, 0.8, 0.8, 0.8});
        ItemsSketch<Double> sketch2 = sketchWithValues(new double[] {0.7, 0.8, 0.8, 0.8, 0.8, 0.8});
        ItemsSketch<Double> sketch3 = sketchWithValues(new double[] {0.7, 0.8, 0.8, 0.8, 0.8, 0.8});
        String valuesString = arrayToValuesString(new String[] {
                sketchToValueString(sketch1),
                sketchToValueString(sketch2),
                sketchToValueString(sketch3)
        });

        String query = "SELECT double_items_sketch_estimate(x, DOUBLE '0.8') FROM " + valuesString + " AS W(x) GROUP BY x";
        MaterializedResult output = runner.execute(query);
        List<MaterializedRow> result = output.getMaterializedRows();
        long value = (long) result.get(0).getFields().get(0);

        Assertions.assertEquals(5, value);
    }
}
