package com.roblox.trino.udfs.datasketches;

import com.roblox.trino.udfs.TrinoUdfsPlugin;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedRow;
import io.trino.testing.StandaloneQueryRunner;
import org.apache.datasketches.kll.KllFloatsSketch;
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
public class KllFloatsTest
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

    private String sparkArrayToValuesString(String[] sketches)
    {
        StringBuilder out = new StringBuilder();
        out.append("(VALUES ");
        for (int i = 0; i < sketches.length; i++) {
            out.append("('").append(sketches[i]).append("')");
            if (i < sketches.length - 1) {
                out.append(", ");
            }
        }
        out.append(")");
        return out.toString();
    }

    private KllFloatsSketch sketchWithValues(double[] values)
    {
        KllFloatsSketch sketch = KllFloatsSketch.newHeapInstance();
        for (double value : values) {
            sketch.update((float) value);
        }
        return sketch;
    }

    private KllFloatsSketch sketchWithValues(double[] values, int k)
    {
        KllFloatsSketch sketch = KllFloatsSketch.newHeapInstance(k);
        for (double value : values) {
            sketch.update((float) value);
        }
        return sketch;
    }

    private String sketchToValueString(KllFloatsSketch sketch)
    {
        byte[] serialized = sketch.toByteArray();
        StringBuilder out = new StringBuilder();

        for (byte b : serialized) {
            out.append(String.format("%02X", b));
        }

        return "(CAST(X'" + out + "' AS VARBINARY))";
    }

    @Test
    public void testQuantileOnSmallValues()
    {
        String valuesString = arrayToValuesString(new Integer[] {10, 5, 15, 10, 9});

        MaterializedResult output = runner.execute("SELECT kll_floats_estimate_quantile(kll_floats_sketch(x), 0.5) FROM " + valuesString + " AS W(x)");
        List<MaterializedRow> result = output.getMaterializedRows();
        double value = (double) result.get(0).getFields().get(0);

        Assertions.assertEquals(10, value);
    }

    @Test
    public void testQuantileOnSmallValuesArray()
    {
        String valuesString = arrayToValuesString(new Integer[] {10, 5, 15, 10, 9});

        MaterializedResult output = runner.execute("SELECT kll_floats_estimate_quantile(kll_floats_sketch(x), ARRAY[0.1, 0.25, 0.5, 0.75, 0.9]) FROM " + valuesString + " AS W(x)");
        List<MaterializedRow> result = output.getMaterializedRows();
        ArrayList<Double> value = (ArrayList<Double>) result.get(0).getFields().get(0);
        ArrayList<Double> expected = new ArrayList<>(List.of(5.0, 9.0, 10.0, 10.0, 15.0));

        Assertions.assertEquals(expected, value);
    }

    @Test
    public void testQuantileOnSmallValuesWithK()
    {
        String valuesString = arrayToValuesString(new Integer[] {10, 5, 15, 10, 9});

        MaterializedResult output = runner.execute("SELECT kll_floats_estimate_quantile(kll_floats_sketch(x, 100), 0.5) FROM " + valuesString + " AS W(x)");
        List<MaterializedRow> result = output.getMaterializedRows();
        double value = (double) result.get(0).getFields().get(0);

        Assertions.assertEquals(10, value);
    }

    @Test
    public void testQuantileOnLargeValues()
    {
        String query = "WITH data AS (SELECT kll_floats_sketch(custkey) AS sketch FROM tpch.sf1.orders) " +
                "SELECT kll_floats_estimate_quantile_lb(sketch, 0.5), kll_floats_estimate_quantile_ub(sketch, 0.5) FROM data";

        MaterializedResult output = runner.execute(query);
        List<MaterializedRow> result = output.getMaterializedRows();
        double lower = (double) result.get(0).getFields().get(0);
        double upper = (double) result.get(0).getFields().get(1);

        double expected = 75464; // 75208
        Assertions.assertTrue(expected >= lower && expected <= upper);
    }

    @Test
    public void testQuantileOnLargeValuesArray()
    {
        String query = "WITH data AS (SELECT kll_floats_sketch(custkey) AS sketch FROM tpch.sf1.orders) " +
                "SELECT kll_floats_estimate_quantile_lb(sketch, ARRAY[0.1, 0.25, 0.5, 0.75, 0.9]), kll_floats_estimate_quantile_ub(sketch, ARRAY[0.1, 0.25, 0.5, 0.75, 0.9]) FROM data";

        MaterializedResult output = runner.execute(query);
        List<MaterializedRow> result = output.getMaterializedRows();
        ArrayList<Double> lower = (ArrayList<Double>) result.get(0).getFields().get(0);
        ArrayList<Double> upper = (ArrayList<Double>) result.get(0).getFields().get(1);
        ArrayList<Double> expected = new ArrayList<>(List.of(15418.0, 37591.0, 75160.0, 112951.0, 135347.0));

        for (int i = 0; i < expected.size(); i++) {
            Assertions.assertTrue(expected.get(i) >= lower.get(i) && expected.get(i) <= upper.get(i));
        }
    }

    @Test
    public void testQuantileOnSketches()
    {
        KllFloatsSketch sketch1 = sketchWithValues(new double[] {1, 2, 2, 3, 4, 5, 5});
        KllFloatsSketch sketch2 = sketchWithValues(new double[] {4, 4, 5, 6, 7, 8, 8});
        KllFloatsSketch sketch3 = sketchWithValues(new double[] {9, 10, 10, 10, 11, 12});

        String valuesString = arrayToValuesString(new String[] {
                sketchToValueString(sketch1),
                sketchToValueString(sketch2),
                sketchToValueString(sketch3)
        });

        String query = "SELECT kll_floats_estimate_quantile(kll_floats_sketch(x), 0.5) FROM " + valuesString + " AS W(x)";
        MaterializedResult output = runner.execute(query);
        List<MaterializedRow> result = output.getMaterializedRows();
        double value = (double) result.get(0).getFields().get(0);

        Assertions.assertEquals(5, value);
    }

    @Test
    public void testQuantileOnSketchesWithK()
    {
        KllFloatsSketch sketch1 = sketchWithValues(new double[] {1, 2, 2, 3, 4, 5, 5}, 100);
        KllFloatsSketch sketch2 = sketchWithValues(new double[] {4, 4, 5, 6, 7, 8, 8}, 100);
        KllFloatsSketch sketch3 = sketchWithValues(new double[] {9, 10, 10, 10, 11, 12}, 100);

        String valuesString = arrayToValuesString(new String[] {
                sketchToValueString(sketch1),
                sketchToValueString(sketch2),
                sketchToValueString(sketch3)
        });

        String query = "SELECT kll_floats_estimate_quantile(kll_floats_sketch(x), 0.5) FROM " + valuesString + " AS W(x)";
        MaterializedResult output = runner.execute(query);
        List<MaterializedRow> result = output.getMaterializedRows();
        double value = (double) result.get(0).getFields().get(0);

        Assertions.assertEquals(5, value);
    }

    @Test
    public void testRankOnSmallValues()
    {
        String valuesString = arrayToValuesString(new Integer[] {10, 5, 15, 10, 9});

        MaterializedResult output = runner.execute("SELECT kll_floats_estimate_rank(kll_floats_sketch(x), 7) FROM " + valuesString + " AS W(x)");
        List<MaterializedRow> result = output.getMaterializedRows();
        double value = (double) result.get(0).getFields().get(0);

        Assertions.assertEquals(0.2, value);
    }

    @Test
    public void testRankOnSmallRealValues()
    {
        String valuesString = "(VALUES (REAL '10.0'), (REAL '5.0'), (REAL '15.0'), (REAL '10.0'), (REAL '9.0'))";

        MaterializedResult output = runner.execute("SELECT kll_floats_estimate_rank(kll_floats_sketch(x), (REAL '7.0')) FROM " + valuesString + " AS W(x)");
        List<MaterializedRow> result = output.getMaterializedRows();
        double value = (double) result.get(0).getFields().get(0);

        Assertions.assertEquals(0.2, value);
    }

    @Test
    public void testRankOnSmallValuesArray()
    {
        String valuesString = arrayToValuesString(new Integer[] {10, 5, 15, 10, 9});

        MaterializedResult output = runner.execute("SELECT kll_floats_estimate_rank(kll_floats_sketch(x), ARRAY[3, 8, 13]) FROM " + valuesString + " AS W(x)");
        List<MaterializedRow> result = output.getMaterializedRows();
        ArrayList<Double> value = (ArrayList<Double>) result.get(0).getFields().get(0);
        ArrayList<Double> expected = new ArrayList<>(List.of(0.0, 0.2, 0.8));

        Assertions.assertEquals(expected, value);
    }

    @Test
    public void testRankOnSmallValuesWithK()
    {
        String valuesString = arrayToValuesString(new Integer[] {10, 5, 15, 10, 9});

        MaterializedResult output = runner.execute("SELECT kll_floats_estimate_rank(kll_floats_sketch(x, 100), 7) FROM " + valuesString + " AS W(x)");
        List<MaterializedRow> result = output.getMaterializedRows();
        double value = (double) result.get(0).getFields().get(0);

        Assertions.assertEquals(0.2, value);
    }

    @Test
    public void testRankOnLargeValues()
    {
        String query = "WITH data AS (SELECT kll_floats_sketch(custkey) AS sketch FROM tpch.sf1.orders) " +
                "SELECT kll_floats_estimate_rank_lb(sketch, 60000), kll_floats_estimate_rank_ub(sketch, 60000) FROM data";

        MaterializedResult output = runner.execute(query);
        List<MaterializedRow> result = output.getMaterializedRows();
        double lower = (double) result.get(0).getFields().get(0);
        double upper = (double) result.get(0).getFields().get(1);
        System.err.println("lower: " + lower + ", upper: " + upper);
        double expected = 0.398568;
        Assertions.assertTrue(expected >= lower && expected <= upper);
    }

    @Test
    public void testRankOnLargeValuesArray()
    {
        String query = "WITH data AS (SELECT kll_floats_sketch(custkey) AS sketch FROM tpch.sf1.orders) " +
                "SELECT kll_floats_estimate_rank_lb(sketch, ARRAY[60000, 80000, 100000]), kll_floats_estimate_rank_ub(sketch, ARRAY[60000, 80000, 100000]) FROM data";

        MaterializedResult output = runner.execute(query);
        List<MaterializedRow> result = output.getMaterializedRows();
        ArrayList<Double> lower = (ArrayList<Double>) result.get(0).getFields().get(0);
        ArrayList<Double> upper = (ArrayList<Double>) result.get(0).getFields().get(1);
        ArrayList<Double> expected = new ArrayList<>(List.of(0.40, 0.53, 0.66));

        for (int i = 0; i < expected.size(); i++) {
            Assertions.assertTrue(expected.get(i) >= lower.get(i) && expected.get(i) <= upper.get(i));
        }
    }

    @Test
    public void testRankOnSketches()
    {
        KllFloatsSketch sketch1 = sketchWithValues(new double[] {1, 2, 2, 3, 4, 5, 5});
        KllFloatsSketch sketch2 = sketchWithValues(new double[] {4, 4, 5, 6, 7, 8, 8});
        KllFloatsSketch sketch3 = sketchWithValues(new double[] {9, 10, 10, 10, 11, 12});

        String valuesString = arrayToValuesString(new String[] {
                sketchToValueString(sketch1),
                sketchToValueString(sketch2),
                sketchToValueString(sketch3)
        });

        String query = "SELECT kll_floats_estimate_rank(kll_floats_sketch(x), 9) FROM " + valuesString + " AS W(x)";
        MaterializedResult output = runner.execute(query);
        List<MaterializedRow> result = output.getMaterializedRows();
        double value = (double) result.get(0).getFields().get(0);

        Assertions.assertEquals(0.75, value);
    }

    @Test
    public void testRankOnSketchesWithK()
    {
        KllFloatsSketch sketch1 = sketchWithValues(new double[] {1, 2, 2, 3, 4, 5, 5}, 100);
        KllFloatsSketch sketch2 = sketchWithValues(new double[] {4, 4, 5, 6, 7, 8, 8}, 100);
        KllFloatsSketch sketch3 = sketchWithValues(new double[] {9, 10, 10, 10, 11, 12}, 100);

        String valuesString = arrayToValuesString(new String[] {
                sketchToValueString(sketch1),
                sketchToValueString(sketch2),
                sketchToValueString(sketch3)
        });

        String query = "SELECT kll_floats_estimate_rank(kll_floats_sketch(x), 9) FROM " + valuesString + " AS W(x)";
        MaterializedResult output = runner.execute(query);
        List<MaterializedRow> result = output.getMaterializedRows();
        double value = (double) result.get(0).getFields().get(0);

        Assertions.assertEquals(0.75, value);
    }

    @Test
    public void testGroupBy()
    {
        KllFloatsSketch sketch1 = sketchWithValues(new double[] {1, 2, 3});
        KllFloatsSketch sketch2 = sketchWithValues(new double[] {1, 2, 3});
        KllFloatsSketch sketch3 = sketchWithValues(new double[] {1, 2, 3});
        String valuesString = arrayToValuesString(new String[] {
                sketchToValueString(sketch1),
                sketchToValueString(sketch2),
                sketchToValueString(sketch3)
        });

        String query = "SELECT kll_floats_estimate_quantile(kll_floats_sketch(x), 0.5) FROM " + valuesString + " AS W(x) GROUP BY x";
        MaterializedResult output = runner.execute(query);
        List<MaterializedRow> result = output.getMaterializedRows();
        double value = (double) result.get(0).getFields().get(0);

        Assertions.assertEquals(2, value);
    }
}
