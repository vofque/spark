/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package test.org.apache.spark.sql;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.test.TestSparkSession;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

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

    private TestSparkSession spark;

    @Before
    public void setUp() {
        spark = new TestSparkSession();
    }

    @After
    public void tearDown() {
        spark.stop();
        spark = null;
    }

    @Test
    public void testBeanWithArrayFieldsDeserialization() {

        StructType schema = createSchema();
        Encoder<Record> encoder = Encoders.bean(Record.class);

        Dataset<Record> dataset = spark
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
