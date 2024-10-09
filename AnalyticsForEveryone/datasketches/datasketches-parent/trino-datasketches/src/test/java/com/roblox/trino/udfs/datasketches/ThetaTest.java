package com.roblox.trino.udfs.datasketches;

import com.roblox.trino.udfs.TrinoUdfsPlugin;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedRow;
import io.trino.testing.QueryFailedException;
import io.trino.testing.StandaloneQueryRunner;
import org.apache.datasketches.theta.UpdateSketch;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.util.List;

import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class ThetaTest
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

    private UpdateSketch sketchWithValues(int[] values)
    {
        UpdateSketch sketch = UpdateSketch.builder().build();
        for (int value : values) {
            sketch.update(value);
        }
        return sketch;
    }

    private UpdateSketch sketchWithValues(int[] values, int k)
    {
        UpdateSketch sketch = UpdateSketch.builder().setNominalEntries(k).build();
        for (int value : values) {
            sketch.update(value);
        }
        return sketch;
    }

    private String sketchToValueString(UpdateSketch sketch)
    {
        byte[] serialized = sketch.compact().toByteArray();
        StringBuilder out = new StringBuilder();

        for (byte b : serialized) {
            out.append(String.format("%02X", b));
        }

        return "(CAST(X'" + out + "' AS VARBINARY))";
    }

    @Test
    public void testOnSmallValues()
    {
        String valuesString = arrayToValuesString(new Integer[] {10, 5, 15, 10, 9});

        MaterializedResult output = runner.execute("SELECT theta_count_distinct(theta_sketch(x)) FROM " + valuesString + " AS W(x)");
        List<MaterializedRow> result = output.getMaterializedRows();
        long value = (long) result.get(0).getFields().get(0);

        Assertions.assertEquals(4, value);
    }

    @Test
    public void testOnSmallRealValues()
    {
        String valuesString = "(VALUES (REAL '10.0'), (REAL '5.0'), (REAL '15.0'), (REAL '10.0'), (REAL '9.0'))";

        MaterializedResult output = runner.execute("SELECT theta_count_distinct(theta_sketch(x)) FROM " + valuesString + " AS W(x)");
        List<MaterializedRow> result = output.getMaterializedRows();
        long value = (long) result.get(0).getFields().get(0);

        Assertions.assertEquals(4, value);
    }

    @Test
    public void testOnSmallValuesWithK()
    {
        String valuesString = arrayToValuesString(new Integer[] {10, 5, 15, 10, 9});

        MaterializedResult output = runner.execute("SELECT theta_count_distinct(theta_sketch(x, 1024)) FROM " + valuesString + " AS W(x)");
        List<MaterializedRow> result = output.getMaterializedRows();
        long value = (long) result.get(0).getFields().get(0);

        Assertions.assertEquals(4, value);
    }

    @Test
    public void testOnLargeValues()
    {
        String query = "WITH data AS (SELECT theta_sketch(custkey) AS sketch FROM tpch.sf1.orders) " +
                "SELECT theta_count_distinct_lb(sketch, 2), theta_count_distinct_ub(sketch, 2) FROM data";
        MaterializedResult output = runner.execute(query);
        List<MaterializedRow> result = output.getMaterializedRows();
        long lower = (long) result.get(0).getFields().get(0);
        long upper = (long) result.get(0).getFields().get(1);

        long expected = 100000;
        Assertions.assertTrue(expected >= lower && expected <= upper);
    }

    @Test
    public void testOnSketches()
    {
        UpdateSketch sketch1 = sketchWithValues(new int[] {1, 2, 2, 3, 4, 5, 5});
        UpdateSketch sketch2 = sketchWithValues(new int[] {4, 4, 5, 6, 7, 8, 8});
        UpdateSketch sketch3 = sketchWithValues(new int[] {9, 10, 10, 10, 11, 12});

        String valuesString = arrayToValuesString(new String[] {
                sketchToValueString(sketch1),
                sketchToValueString(sketch2),
                sketchToValueString(sketch3)
        });

        String query = "SELECT theta_count_distinct(theta_sketch(x)) FROM " + valuesString + " AS W(x)";
        MaterializedResult output = runner.execute(query);
        List<MaterializedRow> result = output.getMaterializedRows();
        long value = (long) result.get(0).getFields().get(0);

        Assertions.assertEquals(12, value);
    }

    @Test
    public void testUnion()
    {
        UpdateSketch sketch1 = sketchWithValues(new int[] {1, 2, 2, 3, 4, 5, 5});
        UpdateSketch sketch2 = sketchWithValues(new int[] {3, 4, 4, 5, 6, 7, 8});

        String valuesString = "(VALUES (" +
                sketchToValueString(sketch1) + ", " +
                sketchToValueString(sketch2) + "))";

        String query = "SELECT theta_count_distinct(theta_union(theta_sketch(x), theta_sketch(y))) FROM " + valuesString + " AS W(x, y)";
        MaterializedResult output = runner.execute(query);
        List<MaterializedRow> result = output.getMaterializedRows();
        long value = (long) result.get(0).getFields().get(0);

        Assertions.assertEquals(8, value);
    }

    @Test
    public void testUnionWithK()
    {
        UpdateSketch sketch1 = sketchWithValues(new int[] {1, 2, 2, 3, 4, 5, 5}, 1024);
        UpdateSketch sketch2 = sketchWithValues(new int[] {3, 4, 4, 5, 6, 7, 8}, 1024);

        String valuesString = "(VALUES (" +
                sketchToValueString(sketch1) + ", " +
                sketchToValueString(sketch2) + "))";

        String query = "SELECT theta_count_distinct(theta_union(theta_sketch(x, 1024), theta_sketch(y, 1024), 1024), 1024) FROM " + valuesString + " AS W(x, y)";
        MaterializedResult output = runner.execute(query);
        List<MaterializedRow> result = output.getMaterializedRows();
        long value = (long) result.get(0).getFields().get(0);

        Assertions.assertEquals(8, value);
    }

    @Test
    public void testIntersection()
    {
        UpdateSketch sketch1 = sketchWithValues(new int[] {1, 2, 2, 3, 4, 5, 5});
        UpdateSketch sketch2 = sketchWithValues(new int[] {3, 4, 4, 5, 6, 7, 8});

        String valuesString = "(VALUES (" +
                sketchToValueString(sketch1) + ", " +
                sketchToValueString(sketch2) + "))";

        String query = "SELECT theta_count_distinct(theta_intersection(theta_sketch(x), theta_sketch(y))) FROM " + valuesString + " AS W(x, y)";
        MaterializedResult output = runner.execute(query);
        List<MaterializedRow> result = output.getMaterializedRows();
        long value = (long) result.get(0).getFields().get(0);

        Assertions.assertEquals(3, value);
    }

    @Test
    public void testIntersectionWithK()
    {
        UpdateSketch sketch1 = sketchWithValues(new int[] {1, 2, 2, 3, 4, 5, 5}, 1024);
        UpdateSketch sketch2 = sketchWithValues(new int[] {3, 4, 4, 5, 6, 7, 8}, 1024);

        String valuesString = "(VALUES (" +
                sketchToValueString(sketch1) + ", " +
                sketchToValueString(sketch2) + "))";

        String query = "SELECT theta_count_distinct(theta_intersection(theta_sketch(x, 1024), theta_sketch(y, 1024), 1024), 1024) FROM " + valuesString + " AS W(x, y)";
        MaterializedResult output = runner.execute(query);
        List<MaterializedRow> result = output.getMaterializedRows();
        long value = (long) result.get(0).getFields().get(0);

        Assertions.assertEquals(3, value);
    }

    @Test
    public void testOnSparkSketches()
    {
        String valuesString = sparkArrayToValuesString(new String[] {
                "AgMDAAAazJMEAAAAAACAP0eiSn1ltjIbR8rvzjKlKR2fhVoQFi6QbIXSzuXK16N8", // 1 2 3 4
                "AgMDAAAazJMEAAAAAACAP0eiSn1ltjIbcRgfms1idzhT7jC+QI0SUp+FWhAWLpBs", // 3 4 5 6
                "AgMDAAAazJMEAAAAAACAP4WNN2B1IC8vU+4wvkCNElLyRNIsjaEbVMXbZSuoOENV", // 6 7 8 9
                "AgMDAAAazJMDAAAAAACAP6xTDNQCPqsu0pCxsLr3ZzkkjP6AYkSFYQ==",         // 10 11 12
        });
        String query = "SELECT theta_count_distinct(theta_sketch(from_base64(x))) FROM " + valuesString + " AS W(x)";
        MaterializedResult output = runner.execute(query);
        List<MaterializedRow> result = output.getMaterializedRows();
        long value = (long) result.get(0).getFields().get(0);

        Assertions.assertEquals(12, value);
    }

    private void invalidSketchQuery()
    {
        String valuesString = sparkArrayToValuesString(new String[] {
                "AgMDAAAazJMEAAAAAACAP0eiSn1ltjIbR8rvzjKlKR2fhVoQFi6QbIXSzuXK16N8",
                "AgMDAAAazJMEAAAAAACAP0eiSn1ltjIbcRgfms1idzhT7jC+QI0SUp+FWhAWLpBs",
                "Tm90IGEgc2tldGNoISAtIFdpdGggbG92ZSwgU3BlbmNlciBMdXR6",
                "V2h5IGRpZCB5b3UgZGVjb2RlIHRoaXMgc3RvcCBwcm9jcmFzdGluYXRpbmc="
        });
        String query = "SELECT theta_count_distinct(theta_sketch(from_base64(x))) FROM " + valuesString + " AS W(x)";
        runner.execute(query);
    }

    @Test
    public void testOnInvalidSparkSketches()
    {
        Assertions.assertThrows(QueryFailedException.class, this::invalidSketchQuery);
    }

    @Test
    public void testGroupBy()
    {
        UpdateSketch sketch1 = sketchWithValues(new int[] {1, 2, 3});
        UpdateSketch sketch2 = sketchWithValues(new int[] {1, 2, 3});
        UpdateSketch sketch3 = sketchWithValues(new int[] {1, 2, 3});
        String valuesString = arrayToValuesString(new String[] {
                sketchToValueString(sketch1),
                sketchToValueString(sketch2),
                sketchToValueString(sketch3)
        });

        String query = "SELECT theta_count_distinct(theta_sketch(x)) FROM " + valuesString + " AS W(x) GROUP BY x";
        MaterializedResult output = runner.execute(query);
        List<MaterializedRow> result = output.getMaterializedRows();
        long value = (long) result.get(0).getFields().get(0);

        Assertions.assertEquals(3, value);
    }
}
