package test.org.apache.spark.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.test.TestSparkSession;
import org.apache.spark.sql.types.*;
import org.junit.Assert;
import org.junit.Test;

import java.util.*;

public class JavaBeanWithMapSuite {

    private static final List<Record> RECORDS = new ArrayList<>();

    static {
        RECORDS.add(new Record(1,
                toMap(
                        Arrays.asList("a", "b"),
                        Arrays.asList(new Interval(111, 211), new Interval(121, 221))
                ),
                toMap(Arrays.asList("a", "b", "c"), Arrays.asList(11, 21, 31))
        ));
        RECORDS.add(new Record(2,
                toMap(
                        Arrays.asList("a", "b"),
                        Arrays.asList(new Interval(112, 212), new Interval(122, 222))
                ),
                toMap(Arrays.asList("a", "b", "c"), Arrays.asList(12, 22, 32))
        ));
        RECORDS.add(new Record(3,
                toMap(
                        Arrays.asList("a", "b"),
                        Arrays.asList(new Interval(113, 213), new Interval(123, 223))
                ),
                toMap(Arrays.asList("a", "b", "c"), Arrays.asList(13, 23, 33))
        ));
    }

    private static <K, V> Map<K, V> toMap(Collection<K> keys, Collection<V> values) {
        Map<K, V> map = new HashMap<>();
        Iterator<K> keyI = keys.iterator();
        Iterator<V> valueI = values.iterator();
        while (keyI.hasNext() && valueI.hasNext()) {
            map.put(keyI.next(), valueI.next());
        }
        return map;
    }

    @Test
    public void testBeanWithMapFieldsDeserialization() {

        SparkSession session = new TestSparkSession();
        StructType schema = createSchema();
        Encoder<Record> encoder = Encoders.bean(Record.class);

        Dataset<Record> dataset = session
                .read()
                .format("json")
                .schema(schema)
                .load("src/test/resources/test-data/with-map-fields")
                .as(encoder);

        List<Record> records = dataset.collectAsList();

        Assert.assertTrue(Util.equals(records, RECORDS));
    }

    private static StructType createSchema() {
        StructField[] intervalFields = {
                new StructField("startTime", DataTypes.LongType, true, Metadata.empty()),
                new StructField("endTime", DataTypes.LongType, true, Metadata.empty())
        };
        DataType intervalType = new StructType(intervalFields);

        DataType intervalsType = new MapType(DataTypes.StringType, intervalType, true);

        DataType valuesType = new MapType(DataTypes.StringType, DataTypes.IntegerType, true);

        StructField[] fields = {
                new StructField("id", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("intervals", intervalsType, true, Metadata.empty()),
                new StructField("values", valuesType, true, Metadata.empty())
        };
        return new StructType(fields);
    }

    public static class Record {

        private int id;
        private Map<String, Interval> intervals;
        private Map<String, Integer> values;

        public Record() { }

        Record(int id, Map<String, Interval> intervals, Map<String, Integer> values) {
            this.id = id;
            this.intervals = intervals;
            this.values = values;
        }

        public int getId() {
            return id;
        }

        public void setId(int id) {
            this.id = id;
        }

        public Map<String, Interval> getIntervals() {
            return intervals;
        }

        public void setIntervals(Map<String, Interval> intervals) {
            this.intervals = intervals;
        }

        public Map<String, Integer> getValues() {
            return values;
        }

        public void setValues(Map<String, Integer> values) {
            this.values = values;
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof Record)) return false;
            Record other = (Record) obj;
            return
                    (other.id == this.id) &&
                    Util.equals(other.intervals, this.intervals) &&
                    Util.equals(other.values, this.values);
        }

        @Override
        public String toString() {
            return String.format("{ id: %d, intervals: %s, values: %s }", id, intervals, values);
        }
    }

    public static class Interval {

        private long startTime;
        private long endTime;

        public Interval() { }

        Interval(long startTime, long endTime) {
            this.startTime = startTime;
            this.endTime = endTime;
        }

        public long getStartTime() {
            return startTime;
        }

        public void setStartTime(long startTime) {
            this.startTime = startTime;
        }

        public long getEndTime() {
            return endTime;
        }

        public void setEndTime(long endTime) {
            this.endTime = endTime;
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof Interval)) return false;
            Interval other = (Interval) obj;
            return
                    (other.startTime == this.startTime) &&
                    (other.endTime == this.endTime);
        }

        @Override
        public String toString() {
            return String.format("[%d,%d]", startTime, endTime);
        }
    }

    private static class Util {

        private static <E> boolean equals(Collection<E> as, Collection<E> bs) {
            if (as.size() != bs.size()) return false;
            Iterator<E> ai = as.iterator();
            Iterator<E> bi = bs.iterator();
            while (ai.hasNext() && bi.hasNext()) {
                if (!ai.next().equals(bi.next())) {
                    return false;
                }
            }
            return true;
        }

        private static <K, V> boolean equals(Map<K, V> aMap, Map<K, V> bMap) {
            if (aMap.size() != bMap.size()) return false;
            for (K key : aMap.keySet()) {
                if (!bMap.containsKey(key)) {
                    return false;
                }
                if (!aMap.get(key).equals(bMap.get(key))) {
                    return false;
                }
            }
            return true;
        }
    }
}

