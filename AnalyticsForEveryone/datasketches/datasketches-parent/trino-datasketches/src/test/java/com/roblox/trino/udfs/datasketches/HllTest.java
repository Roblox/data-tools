package com.roblox.trino.udfs.datasketches;

import com.roblox.trino.udfs.TrinoUdfsPlugin;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedRow;
import io.trino.testing.QueryFailedException;
import io.trino.testing.StandaloneQueryRunner;
import org.apache.datasketches.hll.HllSketch;
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
public class HllTest
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

    private HllSketch sketchWithValues(int[] values)
    {
        HllSketch sketch = new HllSketch();
        for (int value : values) {
            sketch.update(value);
        }
        return sketch;
    }

    private HllSketch sketchWithValues(int[] values, int lgK)
    {
        HllSketch sketch = new HllSketch(lgK);
        for (int value : values) {
            sketch.update(value);
        }
        return sketch;
    }

    private String sketchToValueString(HllSketch sketch)
    {
        byte[] serialized = sketch.toCompactByteArray();
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

        MaterializedResult output = runner.execute("SELECT hll_count_distinct(hll_sketch(x)) FROM " + valuesString + " AS W(x)");
        List<MaterializedRow> result = output.getMaterializedRows();
        long value = (long) result.get(0).getFields().get(0);

        Assertions.assertEquals(4, value);
    }

    @Test
    public void testOnSmallRealValues()
    {
        String valuesString = "(VALUES (REAL '10.0'), (REAL '5.0'), (REAL '15.0'), (REAL '10.0'), (REAL '9.0'))";

        MaterializedResult output = runner.execute("SELECT hll_count_distinct(hll_sketch(x)) FROM " + valuesString + " AS W(x)");
        List<MaterializedRow> result = output.getMaterializedRows();
        long value = (long) result.get(0).getFields().get(0);

        Assertions.assertEquals(4, value);
    }

    @Test
    public void testOnSmallValuesWithLgK()
    {
        String valuesString = arrayToValuesString(new Integer[] {10, 5, 15, 10, 9});

        MaterializedResult output = runner.execute("SELECT hll_count_distinct(hll_sketch(x, 14)) FROM " + valuesString + " AS W(x)");
        List<MaterializedRow> result = output.getMaterializedRows();
        long value = (long) result.get(0).getFields().get(0);

        Assertions.assertEquals(4, value);
    }

    @Test
    public void testOnLargeValues()
    {
        String query = "WITH data AS (SELECT hll_sketch(custkey) AS sketch FROM tpch.sf1.orders) " +
                "SELECT hll_count_distinct_lb(sketch, 2), hll_count_distinct_ub(sketch, 2) FROM data";
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
        HllSketch sketch1 = sketchWithValues(new int[] {1, 2, 2, 3, 4, 5, 5});
        HllSketch sketch2 = sketchWithValues(new int[] {4, 4, 5, 6, 7, 8, 8});
        HllSketch sketch3 = sketchWithValues(new int[] {9, 10, 10, 10, 11, 12});

        String valuesString = arrayToValuesString(new String[] {
                sketchToValueString(sketch1),
                sketchToValueString(sketch2),
                sketchToValueString(sketch3)
        });

        String query = "SELECT hll_count_distinct(hll_sketch(x)) FROM " + valuesString + " AS W(x)";
        MaterializedResult output = runner.execute(query);
        List<MaterializedRow> result = output.getMaterializedRows();
        long value = (long) result.get(0).getFields().get(0);

        Assertions.assertEquals(12, value);
    }

    @Test
    public void testOnSketchesWithLgK()
    {
        HllSketch sketch1 = sketchWithValues(new int[] {1, 2, 2, 3, 4, 5, 5}, 9);
        HllSketch sketch2 = sketchWithValues(new int[] {4, 4, 5, 6, 7, 8, 8}, 9);
        HllSketch sketch3 = sketchWithValues(new int[] {9, 10, 10, 10, 11, 12}, 9);

        String valuesString = arrayToValuesString(new String[] {
                sketchToValueString(sketch1),
                sketchToValueString(sketch2),
                sketchToValueString(sketch3)
        });

        String query = "SELECT hll_count_distinct(hll_sketch(x)) FROM " + valuesString + " AS W(x)";
        MaterializedResult output = runner.execute(query);
        List<MaterializedRow> result = output.getMaterializedRows();
        long value = (long) result.get(0).getFields().get(0);

        Assertions.assertEquals(12, value);
    }

    @Test
    public void testOnSparkSketches()
    {
        String valuesString = sparkArrayToValuesString(new String[] {
                "AgEHDgMIAQjQskEU",
                "AgEHDgMIAQjGJc0H",
                "AwEHDgUIAAkUAAAAhfW6DmZE9AuS/h4FF8qEDyt+bQysva4IDk1OBY+XawQsm3YF0TzBBtLs+gQ0vHYJ9j92BWvRZQp35RcFT0wYCXpS3glbNs8FXFc4B556aBg=",
                "AwEHDgUIAAkLAAAAAAPoBEV+PwrIJ8kE7VWtC4/tTAq0o9sO+dLkD5qDSw98gI8O/YDvDr8F4QY="
        });
        String query = "SELECT hll_count_distinct(hll_sketch(from_base64(x))) FROM " + valuesString + " AS W(x)";
        MaterializedResult output = runner.execute(query);
        List<MaterializedRow> result = output.getMaterializedRows();
        long value = (long) result.get(0).getFields().get(0);

        Assertions.assertEquals(33, value);
    }

    private void invalidSketchQuery()
    {
        String valuesString = sparkArrayToValuesString(new String[] {
                "AgEHDgMIAQjQskEU",
                "AgEHDgMIAQjGJc0H",
                "Tm90IGEgc2tldGNoISAtIFdpdGggbG92ZSwgU3BlbmNlciBMdXR6",
                "V2h5IGRpZCB5b3UgZGVjb2RlIHRoaXMgc3RvcCBwcm9jcmFzdGluYXRpbmc="
        });
        String query = "SELECT hll_count_distinct(hll_sketch(from_base64(x))) FROM " + valuesString + " AS W(x)";
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
        HllSketch sketch1 = sketchWithValues(new int[] {1, 2, 3});
        HllSketch sketch2 = sketchWithValues(new int[] {1, 2, 3});
        HllSketch sketch3 = sketchWithValues(new int[] {1, 2, 3});
        String valuesString = arrayToValuesString(new String[] {
                sketchToValueString(sketch1),
                sketchToValueString(sketch2),
                sketchToValueString(sketch3)
        });

        String query = "SELECT hll_count_distinct(hll_sketch(x)) FROM " + valuesString + " AS W(x) GROUP BY x";
        MaterializedResult output = runner.execute(query);
        List<MaterializedRow> result = output.getMaterializedRows();
        long value = (long) result.get(0).getFields().get(0);

        Assertions.assertEquals(3, value);
    }
}
