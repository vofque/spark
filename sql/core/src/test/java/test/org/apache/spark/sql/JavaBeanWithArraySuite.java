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

public class JavaBeanWithArraySuite {

    private static final List<Record> RECORDS = new ArrayList<>();

    static {
        RECORDS.add(new Record(1,
                Arrays.asList(new Interval(111, 211), new Interval(121, 221)),
                Arrays.asList(11, 21, 31, 41)
        ));
        RECORDS.add(new Record(2,
                Arrays.asList(new Interval(112, 212), new Interval(122, 222)),
                Arrays.asList(12, 22, 32, 42)
        ));
        RECORDS.add(new Record(3,
                Arrays.asList(new Interval(113, 213), new Interval(123, 223)),
                Arrays.asList(13, 23, 33, 43)
        ));
    }

    @Test
    public void testBeanWithArrayFieldsDeserialization() {

        SparkSession session = new TestSparkSession();
        StructType schema = createSchema();
        Encoder<Record> encoder = Encoders.bean(Record.class);

        Dataset<Record> dataset = session
                .read()
                .format("json")
                .schema(schema)
                .load("src/test/resources/test-data/with-array-fields")
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

        DataType intervalsType = new ArrayType(intervalType, true);

        DataType valuesType = new ArrayType(DataTypes.IntegerType, true);

        StructField[] fields = {
                new StructField("id", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("intervals", intervalsType, true, Metadata.empty()),
                new StructField("values", valuesType, true, Metadata.empty())
        };
        return new StructType(fields);
    }

    public static class Record {

        private int id;
        private List<Interval> intervals;
        private List<Integer> values;

        public Record() { }

        Record(int id, List<Interval> intervals, List<Integer> values) {
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

        public List<Interval> getIntervals() {
            return intervals;
        }

        public void setIntervals(List<Interval> intervals) {
            this.intervals = intervals;
        }

        public List<Integer> getValues() {
            return values;
        }

        public void setValues(List<Integer> values) {
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
            return String.format("{ id: %d, intervals: %s }", id, intervals );
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
    }
}
