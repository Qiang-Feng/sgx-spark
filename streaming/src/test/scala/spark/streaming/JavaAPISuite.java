package spark.streaming;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import scala.Tuple2;
import spark.HashPartitioner;
import spark.api.java.JavaRDD;
import spark.api.java.function.FlatMapFunction;
import spark.api.java.function.Function;
import spark.api.java.function.Function2;
import spark.api.java.function.PairFunction;
import spark.storage.StorageLevel;
import spark.streaming.api.java.JavaDStream;
import spark.streaming.api.java.JavaPairDStream;
import spark.streaming.api.java.JavaStreamingContext;
import spark.streaming.JavaTestUtils;
import spark.streaming.dstream.KafkaPartitionKey;
import sun.org.mozilla.javascript.annotations.JSFunction;

import java.io.*;
import java.util.*;

// The test suite itself is Serializable so that anonymous Function implementations can be
// serialized, as an alternative to converting these anonymous classes to static inner classes;
// see http://stackoverflow.com/questions/758570/.
public class JavaAPISuite implements Serializable {
  private transient JavaStreamingContext sc;

  @Before
  public void setUp() {
    sc = new JavaStreamingContext("local[2]", "test", new Duration(1000));
  }

  @After
  public void tearDown() {
    sc.stop();
    sc = null;
    // To avoid Akka rebinding to the same port, since it doesn't unbind immediately on shutdown
    System.clearProperty("spark.master.port");
  }

  @Test
  public void testCount() {
    List<List<Integer>> inputData = Arrays.asList(
        Arrays.asList(1,2,3,4),
        Arrays.asList(3,4,5),
        Arrays.asList(3));

    List<List<Integer>> expected = Arrays.asList(
        Arrays.asList(4),
        Arrays.asList(3),
        Arrays.asList(1));

    JavaDStream stream = JavaTestUtils.attachTestInputStream(sc, inputData, 1);
    JavaDStream count = stream.count();
    JavaTestUtils.attachTestOutputStream(count);
    List<List<Integer>> result = JavaTestUtils.runStreams(sc, 3, 3);
    assertOrderInvariantEquals(expected, result);
  }

  @Test
  public void testMap() {
    List<List<String>> inputData = Arrays.asList(
        Arrays.asList("hello", "world"),
        Arrays.asList("goodnight", "moon"));

   List<List<Integer>> expected = Arrays.asList(
        Arrays.asList(5,5),
        Arrays.asList(9,4));

    JavaDStream stream = JavaTestUtils.attachTestInputStream(sc, inputData, 1);
    JavaDStream letterCount = stream.map(new Function<String, Integer>() {
        @Override
        public Integer call(String s) throws Exception {
          return s.length();
        }
    });
    JavaTestUtils.attachTestOutputStream(letterCount);
    List<List<Integer>> result = JavaTestUtils.runStreams(sc, 2, 2);

    assertOrderInvariantEquals(expected, result);
  }

  @Test
  public void testWindow() {
    List<List<Integer>> inputData = Arrays.asList(
        Arrays.asList(1,2,3),
        Arrays.asList(4,5,6),
        Arrays.asList(7,8,9));

    List<List<Integer>> expected = Arrays.asList(
        Arrays.asList(1,2,3),
        Arrays.asList(4,5,6,1,2,3),
        Arrays.asList(7,8,9,4,5,6),
        Arrays.asList(7,8,9));

    JavaDStream stream = JavaTestUtils.attachTestInputStream(sc, inputData, 1);
    JavaDStream windowed = stream.window(new Duration(2000));
    JavaTestUtils.attachTestOutputStream(windowed);
    List<List<Integer>> result = JavaTestUtils.runStreams(sc, 4, 4);

    assertOrderInvariantEquals(expected, result);
  }

  @Test
  public void testWindowWithSlideDuration() {
    List<List<Integer>> inputData = Arrays.asList(
        Arrays.asList(1,2,3),
        Arrays.asList(4,5,6),
        Arrays.asList(7,8,9),
        Arrays.asList(10,11,12),
        Arrays.asList(13,14,15),
        Arrays.asList(16,17,18));

    List<List<Integer>> expected = Arrays.asList(
        Arrays.asList(1,2,3,4,5,6),
        Arrays.asList(1,2,3,4,5,6,7,8,9,10,11,12),
        Arrays.asList(7,8,9,10,11,12,13,14,15,16,17,18),
        Arrays.asList(13,14,15,16,17,18));

    JavaDStream stream = JavaTestUtils.attachTestInputStream(sc, inputData, 1);
    JavaDStream windowed = stream.window(new Duration(4000), new Duration(2000));
    JavaTestUtils.attachTestOutputStream(windowed);
    List<List<Integer>> result = JavaTestUtils.runStreams(sc, 8, 4);

    assertOrderInvariantEquals(expected, result);
  }

  @Test
  public void testTumble() {
    List<List<Integer>> inputData = Arrays.asList(
        Arrays.asList(1,2,3),
        Arrays.asList(4,5,6),
        Arrays.asList(7,8,9),
        Arrays.asList(10,11,12),
        Arrays.asList(13,14,15),
        Arrays.asList(16,17,18));

    List<List<Integer>> expected = Arrays.asList(
        Arrays.asList(1,2,3,4,5,6),
        Arrays.asList(7,8,9,10,11,12),
        Arrays.asList(13,14,15,16,17,18));

    JavaDStream stream = JavaTestUtils.attachTestInputStream(sc, inputData, 1);
    JavaDStream windowed = stream.tumble(new Duration(2000));
    JavaTestUtils.attachTestOutputStream(windowed);
    List<List<Integer>> result = JavaTestUtils.runStreams(sc, 6, 3);

    assertOrderInvariantEquals(expected, result);
  }

  @Test
  public void testFilter() {
    List<List<String>> inputData = Arrays.asList(
        Arrays.asList("giants", "dodgers"),
        Arrays.asList("yankees", "red socks"));

    List<List<String>> expected = Arrays.asList(
        Arrays.asList("giants"),
        Arrays.asList("yankees"));

    JavaDStream stream = JavaTestUtils.attachTestInputStream(sc, inputData, 1);
    JavaDStream filtered = stream.filter(new Function<String, Boolean>() {
      @Override
      public Boolean call(String s) throws Exception {
        return s.contains("a");
      }
    });
    JavaTestUtils.attachTestOutputStream(filtered);
    List<List<String>> result = JavaTestUtils.runStreams(sc, 2, 2);

    assertOrderInvariantEquals(expected, result);
  }

  @Test
  public void testGlom() {
    List<List<String>> inputData = Arrays.asList(
        Arrays.asList("giants", "dodgers"),
        Arrays.asList("yankees", "red socks"));

    List<List<List<String>>> expected = Arrays.asList(
        Arrays.asList(Arrays.asList("giants", "dodgers")),
        Arrays.asList(Arrays.asList("yankees", "red socks")));

    JavaDStream stream = JavaTestUtils.attachTestInputStream(sc, inputData, 1);
    JavaDStream glommed = stream.glom();
    JavaTestUtils.attachTestOutputStream(glommed);
    List<List<List<String>>> result = JavaTestUtils.runStreams(sc, 2, 2);

    Assert.assertEquals(expected, result);
  }

  @Test
  public void testMapPartitions() {
    List<List<String>> inputData = Arrays.asList(
        Arrays.asList("giants", "dodgers"),
        Arrays.asList("yankees", "red socks"));

    List<List<String>> expected = Arrays.asList(
        Arrays.asList("GIANTSDODGERS"),
        Arrays.asList("YANKEESRED SOCKS"));

    JavaDStream stream = JavaTestUtils.attachTestInputStream(sc, inputData, 1);
    JavaDStream mapped = stream.mapPartitions(new FlatMapFunction<Iterator<String>, String>() {
      @Override
      public Iterable<String> call(Iterator<String> in) {
        String out = "";
        while (in.hasNext()) {
          out = out + in.next().toUpperCase();
        }
        return Lists.newArrayList(out);
      }
    });
    JavaTestUtils.attachTestOutputStream(mapped);
    List<List<List<String>>> result = JavaTestUtils.runStreams(sc, 2, 2);

    Assert.assertEquals(expected, result);
  }

  private class IntegerSum extends Function2<Integer, Integer, Integer> {
    @Override
    public Integer call(Integer i1, Integer i2) throws Exception {
      return i1 + i2;
    }
  }

  private class IntegerDifference extends Function2<Integer, Integer, Integer> {
    @Override
    public Integer call(Integer i1, Integer i2) throws Exception {
      return i1 - i2;
    }
  }

  @Test
  public void testReduce() {
    List<List<Integer>> inputData = Arrays.asList(
        Arrays.asList(1,2,3),
        Arrays.asList(4,5,6),
        Arrays.asList(7,8,9));

    List<List<Integer>> expected = Arrays.asList(
        Arrays.asList(6),
        Arrays.asList(15),
        Arrays.asList(24));

    JavaDStream stream = JavaTestUtils.attachTestInputStream(sc, inputData, 1);
    JavaDStream reduced = stream.reduce(new IntegerSum());
    JavaTestUtils.attachTestOutputStream(reduced);
    List<List<Integer>> result = JavaTestUtils.runStreams(sc, 3, 3);

    Assert.assertEquals(expected, result);
  }

  @Test
  public void testReduceByWindow() {
    List<List<Integer>> inputData = Arrays.asList(
        Arrays.asList(1,2,3),
        Arrays.asList(4,5,6),
        Arrays.asList(7,8,9));

    List<List<Integer>> expected = Arrays.asList(
        Arrays.asList(6),
        Arrays.asList(21),
        Arrays.asList(39),
        Arrays.asList(24));

    JavaDStream stream = JavaTestUtils.attachTestInputStream(sc, inputData, 1);
    JavaDStream reducedWindowed = stream.reduceByWindow(new IntegerSum(),
        new IntegerDifference(), new Duration(2000), new Duration(1000));
    JavaTestUtils.attachTestOutputStream(reducedWindowed);
    List<List<Integer>> result = JavaTestUtils.runStreams(sc, 4, 4);

    Assert.assertEquals(expected, result);
  }

  @Test
  public void testTransform() {
    List<List<Integer>> inputData = Arrays.asList(
        Arrays.asList(1,2,3),
        Arrays.asList(4,5,6),
        Arrays.asList(7,8,9));

    List<List<Integer>> expected = Arrays.asList(
        Arrays.asList(3,4,5),
        Arrays.asList(6,7,8),
        Arrays.asList(9,10,11));

    JavaDStream stream = JavaTestUtils.attachTestInputStream(sc, inputData, 1);
    JavaDStream transformed = stream.transform(new Function<JavaRDD<Integer>, JavaRDD<Integer>>() {
      @Override
      public JavaRDD<Integer> call(JavaRDD<Integer> in) throws Exception {
        return in.map(new Function<Integer, Integer>() {
          @Override
          public Integer call(Integer i) throws Exception {
            return i + 2;
          }
        });
      }});
    JavaTestUtils.attachTestOutputStream(transformed);
    List<List<Integer>> result = JavaTestUtils.runStreams(sc, 3, 3);

    assertOrderInvariantEquals(expected, result);
  }

  @Test
  public void testUnion() {
    List<List<Integer>> inputData1 = Arrays.asList(
        Arrays.asList(1,1),
        Arrays.asList(2,2),
        Arrays.asList(3,3));

    List<List<Integer>> inputData2 = Arrays.asList(
        Arrays.asList(4,4),
        Arrays.asList(5,5),
        Arrays.asList(6,6));

    List<List<Integer>> expected = Arrays.asList(
        Arrays.asList(1,1,4,4),
        Arrays.asList(2,2,5,5),
        Arrays.asList(3,3,6,6));

    JavaDStream stream1 = JavaTestUtils.attachTestInputStream(sc, inputData1, 2);
    JavaDStream stream2 = JavaTestUtils.attachTestInputStream(sc, inputData2, 2);

    JavaDStream unioned = stream1.union(stream2);
    JavaTestUtils.attachTestOutputStream(unioned);
    List<List<Integer>> result = JavaTestUtils.runStreams(sc, 3, 3);

    assertOrderInvariantEquals(expected, result);
  }

  /*
   * Performs an order-invariant comparison of lists representing two RDD streams. This allows
   * us to account for ordering variation within individual RDD's which occurs during windowing.
   */
  public static <T extends Comparable> void assertOrderInvariantEquals(
      List<List<T>> expected, List<List<T>> actual) {
    for (List<T> list: expected) {
      Collections.sort(list);
    }
    for (List<T> list: actual) {
      Collections.sort(list);
    }
    Assert.assertEquals(expected, actual);
  }


  // PairDStream Functions
  @Test
  public void testPairFilter() {
    List<List<String>> inputData = Arrays.asList(
        Arrays.asList("giants", "dodgers"),
        Arrays.asList("yankees", "red socks"));

    List<List<Tuple2<String, Integer>>> expected = Arrays.asList(
        Arrays.asList(new Tuple2<String, Integer>("giants", 6)),
        Arrays.asList(new Tuple2<String, Integer>("yankees", 7)));

    JavaDStream stream = JavaTestUtils.attachTestInputStream(sc, inputData, 1);
    JavaPairDStream<String, Integer> pairStream = stream.map(
        new PairFunction<String, String, Integer>() {
          @Override
          public Tuple2 call(String in) throws Exception {
            return new Tuple2<String, Integer>(in, in.length());
          }
        });

    JavaPairDStream<String, Integer> filtered = pairStream.filter(
        new Function<Tuple2<String, Integer>, Boolean>() {
      @Override
      public Boolean call(Tuple2<String, Integer> in) throws Exception {
        return in._1().contains("a");
      }
    });
    JavaTestUtils.attachTestOutputStream(filtered);
    List<List<Tuple2<String, Integer>>> result = JavaTestUtils.runStreams(sc, 2, 2);

    Assert.assertEquals(expected, result);
  }

  List<List<Tuple2<String, String>>> stringStringKVStream = Arrays.asList(
      Arrays.asList(new Tuple2<String, String>("california", "dodgers"),
          new Tuple2<String, String>("california", "giants"),
          new Tuple2<String, String>("new york", "yankees"),
          new Tuple2<String, String>("new york", "mets")),
      Arrays.asList(new Tuple2<String, String>("california", "sharks"),
          new Tuple2<String, String>("california", "ducks"),
          new Tuple2<String, String>("new york", "rangers"),
          new Tuple2<String, String>("new york", "islanders")));

  List<List<Tuple2<String, Integer>>> stringIntKVStream = Arrays.asList(
      Arrays.asList(
          new Tuple2<String, Integer>("california", 1),
          new Tuple2<String, Integer>("california", 3),
          new Tuple2<String, Integer>("new york", 4),
          new Tuple2<String, Integer>("new york", 1)),
      Arrays.asList(
          new Tuple2<String, Integer>("california", 5),
          new Tuple2<String, Integer>("california", 5),
          new Tuple2<String, Integer>("new york", 3),
          new Tuple2<String, Integer>("new york", 1)));

  @Test
  public void testPairGroupByKey() {
    List<List<Tuple2<String, String>>> inputData = stringStringKVStream;

    List<List<Tuple2<String, List<String>>>> expected = Arrays.asList(
        Arrays.asList(
            new Tuple2<String, List<String>>("california", Arrays.asList("dodgers", "giants")),
            new Tuple2<String, List<String>>("new york", Arrays.asList("yankees", "mets"))),
        Arrays.asList(
            new Tuple2<String, List<String>>("california", Arrays.asList("sharks", "ducks")),
            new Tuple2<String, List<String>>("new york", Arrays.asList("rangers", "islanders"))));

    JavaDStream<Tuple2<String, String>> stream = JavaTestUtils.attachTestInputStream(sc, inputData, 1);
    JavaPairDStream<String, String> pairStream = JavaPairDStream.fromJavaDStream(stream);

    JavaPairDStream<String, List<String>> grouped = pairStream.groupByKey();
    JavaTestUtils.attachTestOutputStream(grouped);
    List<List<Tuple2<String, List<String>>>> result = JavaTestUtils.runStreams(sc, 2, 2);

    Assert.assertEquals(expected, result);
  }

  @Test
  public void testPairReduceByKey() {
    List<List<Tuple2<String, Integer>>> inputData = stringIntKVStream;

    List<List<Tuple2<String, Integer>>> expected = Arrays.asList(
        Arrays.asList(
            new Tuple2<String, Integer>("california", 4),
            new Tuple2<String, Integer>("new york", 5)),
        Arrays.asList(
            new Tuple2<String, Integer>("california", 10),
            new Tuple2<String, Integer>("new york", 4)));

    JavaDStream<Tuple2<String, Integer>> stream = JavaTestUtils.attachTestInputStream(
        sc, inputData, 1);
    JavaPairDStream<String, Integer> pairStream = JavaPairDStream.fromJavaDStream(stream);

    JavaPairDStream<String, Integer> reduced = pairStream.reduceByKey(new IntegerSum());

    JavaTestUtils.attachTestOutputStream(reduced);
    List<List<Tuple2<String, Integer>>> result = JavaTestUtils.runStreams(sc, 2, 2);

    Assert.assertEquals(expected, result);
  }

  @Test
  public void testCombineByKey() {
    List<List<Tuple2<String, Integer>>> inputData = stringIntKVStream;

    List<List<Tuple2<String, Integer>>> expected = Arrays.asList(
        Arrays.asList(
            new Tuple2<String, Integer>("california", 4),
            new Tuple2<String, Integer>("new york", 5)),
        Arrays.asList(
            new Tuple2<String, Integer>("california", 10),
            new Tuple2<String, Integer>("new york", 4)));

    JavaDStream<Tuple2<String, Integer>> stream = JavaTestUtils.attachTestInputStream(
        sc, inputData, 1);
    JavaPairDStream<String, Integer> pairStream = JavaPairDStream.fromJavaDStream(stream);

    JavaPairDStream<String, Integer> combined = pairStream.<Integer>combineByKey(
        new Function<Integer, Integer>() {
          @Override
          public Integer call(Integer i) throws Exception {
            return i;
          }
        }, new IntegerSum(), new IntegerSum(), new HashPartitioner(2));

    JavaTestUtils.attachTestOutputStream(combined);
    List<List<Tuple2<String, Integer>>> result = JavaTestUtils.runStreams(sc, 2, 2);

    Assert.assertEquals(expected, result);
  }

  @Test
  public void testCountByKey() {
    List<List<Tuple2<String, String>>> inputData = stringStringKVStream;

    List<List<Tuple2<String, Long>>> expected = Arrays.asList(
        Arrays.asList(
            new Tuple2<String, Long>("california", 2L),
            new Tuple2<String, Long>("new york", 2L)),
        Arrays.asList(
            new Tuple2<String, Long>("california", 2L),
            new Tuple2<String, Long>("new york", 2L)));

    JavaDStream<Tuple2<String, String>> stream = JavaTestUtils.attachTestInputStream(
        sc, inputData, 1);
    JavaPairDStream<String, String> pairStream = JavaPairDStream.fromJavaDStream(stream);

    JavaPairDStream<String, Long> counted = pairStream.countByKey();
    JavaTestUtils.attachTestOutputStream(counted);
    List<List<Tuple2<String, Long>>> result = JavaTestUtils.runStreams(sc, 2, 2);

    Assert.assertEquals(expected, result);
  }

  @Test
  public void testGroupByKeyAndWindow() {
    List<List<Tuple2<String, String>>> inputData = stringStringKVStream;

    List<List<Tuple2<String, List<String>>>> expected = Arrays.asList(
        Arrays.asList(new Tuple2<String, List<String>>("california", Arrays.asList("dodgers", "giants")),
          new Tuple2<String, List<String>>("new york", Arrays.asList("yankees", "mets"))),
        Arrays.asList(new Tuple2<String, List<String>>("california",
            Arrays.asList("sharks", "ducks", "dodgers", "giants")),
            new Tuple2<String, List<String>>("new york", Arrays.asList("rangers", "islanders", "yankees", "mets"))),
        Arrays.asList(new Tuple2<String, List<String>>("california", Arrays.asList("sharks", "ducks")),
            new Tuple2<String, List<String>>("new york", Arrays.asList("rangers", "islanders"))));

    JavaDStream<Tuple2<String, String>> stream = JavaTestUtils.attachTestInputStream(sc, inputData, 1);
    JavaPairDStream<String, String> pairStream = JavaPairDStream.fromJavaDStream(stream);

    JavaPairDStream<String, List<String>> groupWindowed =
        pairStream.groupByKeyAndWindow(new Duration(2000), new Duration(1000));
    JavaTestUtils.attachTestOutputStream(groupWindowed);
    List<List<Tuple2<String, List<String>>>> result = JavaTestUtils.runStreams(sc, 3, 3);

    Assert.assertEquals(expected, result);
  }

  @Test
  public void testReduceByKeyAndWindow() {
    List<List<Tuple2<String, Integer>>> inputData = stringIntKVStream;

    List<List<Tuple2<String, Integer>>> expected = Arrays.asList(
        Arrays.asList(new Tuple2<String, Integer>("california", 4),
            new Tuple2<String, Integer>("new york", 5)),
        Arrays.asList(new Tuple2<String, Integer>("california", 14),
            new Tuple2<String, Integer>("new york", 9)),
        Arrays.asList(new Tuple2<String, Integer>("california", 10),
            new Tuple2<String, Integer>("new york", 4)));

    JavaDStream<Tuple2<String, Integer>> stream = JavaTestUtils.attachTestInputStream(sc, inputData, 1);
    JavaPairDStream<String, Integer> pairStream = JavaPairDStream.fromJavaDStream(stream);

    JavaPairDStream<String, Integer> reduceWindowed =
        pairStream.reduceByKeyAndWindow(new IntegerSum(), new Duration(2000), new Duration(1000));
    JavaTestUtils.attachTestOutputStream(reduceWindowed);
    List<List<Tuple2<String, Integer>>> result = JavaTestUtils.runStreams(sc, 3, 3);

    Assert.assertEquals(expected, result);
  }

  @Test
  public void testReduceByKeyAndWindowWithInverse() {
    List<List<Tuple2<String, Integer>>> inputData = stringIntKVStream;

    List<List<Tuple2<String, Integer>>> expected = Arrays.asList(
        Arrays.asList(new Tuple2<String, Integer>("california", 4),
            new Tuple2<String, Integer>("new york", 5)),
        Arrays.asList(new Tuple2<String, Integer>("california", 14),
            new Tuple2<String, Integer>("new york", 9)),
        Arrays.asList(new Tuple2<String, Integer>("california", 10),
            new Tuple2<String, Integer>("new york", 4)));

    JavaDStream<Tuple2<String, Integer>> stream = JavaTestUtils.attachTestInputStream(sc, inputData, 1);
    JavaPairDStream<String, Integer> pairStream = JavaPairDStream.fromJavaDStream(stream);

    JavaPairDStream<String, Integer> reduceWindowed =
        pairStream.reduceByKeyAndWindow(new IntegerSum(), new IntegerDifference(), new Duration(2000), new Duration(1000));
    JavaTestUtils.attachTestOutputStream(reduceWindowed);
    List<List<Tuple2<String, Integer>>> result = JavaTestUtils.runStreams(sc, 3, 3);

    Assert.assertEquals(expected, result);
  }

  @Test
  public void testCountByKeyAndWindow() {
    List<List<Tuple2<String, String>>> inputData = stringStringKVStream;

    List<List<Tuple2<String, Long>>> expected = Arrays.asList(
        Arrays.asList(
            new Tuple2<String, Long>("california", 2L),
            new Tuple2<String, Long>("new york", 2L)),
        Arrays.asList(
            new Tuple2<String, Long>("california", 4L),
            new Tuple2<String, Long>("new york", 4L)),
        Arrays.asList(
            new Tuple2<String, Long>("california", 2L),
            new Tuple2<String, Long>("new york", 2L)));

    JavaDStream<Tuple2<String, String>> stream = JavaTestUtils.attachTestInputStream(
        sc, inputData, 1);
    JavaPairDStream<String, String> pairStream = JavaPairDStream.fromJavaDStream(stream);

    JavaPairDStream<String, Long> counted =
        pairStream.countByKeyAndWindow(new Duration(2000), new Duration(1000));
    JavaTestUtils.attachTestOutputStream(counted);
    List<List<Tuple2<String, Long>>> result = JavaTestUtils.runStreams(sc, 3, 3);

    Assert.assertEquals(expected, result);
  }

  @Test
  public void testMapValues() {
    List<List<Tuple2<String, String>>> inputData = stringStringKVStream;

    List<List<Tuple2<String, String>>> expected = Arrays.asList(
        Arrays.asList(new Tuple2<String, String>("california", "DODGERS"),
            new Tuple2<String, String>("california", "GIANTS"),
            new Tuple2<String, String>("new york", "YANKEES"),
            new Tuple2<String, String>("new york", "METS")),
        Arrays.asList(new Tuple2<String, String>("california", "SHARKS"),
            new Tuple2<String, String>("california", "DUCKS"),
            new Tuple2<String, String>("new york", "RANGERS"),
            new Tuple2<String, String>("new york", "ISLANDERS")));

    JavaDStream<Tuple2<String, String>> stream = JavaTestUtils.attachTestInputStream(
        sc, inputData, 1);
    JavaPairDStream<String, String> pairStream = JavaPairDStream.fromJavaDStream(stream);

    JavaPairDStream<String, String> mapped = pairStream.mapValues(new Function<String, String>() {
      @Override
      public String call(String s) throws Exception {
        return s.toUpperCase();
      }
    });

    JavaTestUtils.attachTestOutputStream(mapped);
    List<List<Tuple2<String, String>>> result = JavaTestUtils.runStreams(sc, 2, 2);

    Assert.assertEquals(expected, result);
  }

  @Test
  public void testFlatMapValues() {
    List<List<Tuple2<String, String>>> inputData = stringStringKVStream;

    List<List<Tuple2<String, String>>> expected = Arrays.asList(
        Arrays.asList(new Tuple2<String, String>("california", "dodgers1"),
            new Tuple2<String, String>("california", "dodgers2"),
            new Tuple2<String, String>("california", "giants1"),
            new Tuple2<String, String>("california", "giants2"),
            new Tuple2<String, String>("new york", "yankees1"),
            new Tuple2<String, String>("new york", "yankees2"),
            new Tuple2<String, String>("new york", "mets1"),
            new Tuple2<String, String>("new york", "mets2")),
        Arrays.asList(new Tuple2<String, String>("california", "sharks1"),
            new Tuple2<String, String>("california", "sharks2"),
            new Tuple2<String, String>("california", "ducks1"),
            new Tuple2<String, String>("california", "ducks2"),
            new Tuple2<String, String>("new york", "rangers1"),
            new Tuple2<String, String>("new york", "rangers2"),
            new Tuple2<String, String>("new york", "islanders1"),
            new Tuple2<String, String>("new york", "islanders2")));

    JavaDStream<Tuple2<String, String>> stream = JavaTestUtils.attachTestInputStream(
        sc, inputData, 1);
    JavaPairDStream<String, String> pairStream = JavaPairDStream.fromJavaDStream(stream);


    JavaPairDStream<String, String> flatMapped = pairStream.flatMapValues(
        new Function<String, Iterable<String>>() {
          @Override
          public Iterable<String> call(String in) {
            List<String> out = new ArrayList<String>();
            out.add(in + "1");
            out.add(in + "2");
            return out;
          }
        });

    JavaTestUtils.attachTestOutputStream(flatMapped);
    List<List<Tuple2<String, String>>> result = JavaTestUtils.runStreams(sc, 2, 2);

    Assert.assertEquals(expected, result);
  }

  @Test
  public void testCoGroup() {
    List<List<Tuple2<String, String>>> stringStringKVStream1 = Arrays.asList(
        Arrays.asList(new Tuple2<String, String>("california", "dodgers"),
            new Tuple2<String, String>("new york", "yankees")),
        Arrays.asList(new Tuple2<String, String>("california", "sharks"),
            new Tuple2<String, String>("new york", "rangers")));

    List<List<Tuple2<String, String>>> stringStringKVStream2 = Arrays.asList(
        Arrays.asList(new Tuple2<String, String>("california", "giants"),
            new Tuple2<String, String>("new york", "mets")),
        Arrays.asList(new Tuple2<String, String>("california", "ducks"),
            new Tuple2<String, String>("new york", "islanders")));


    List<List<Tuple2<String, Tuple2<List<String>, List<String>>>>> expected = Arrays.asList(
        Arrays.asList(
            new Tuple2<String, Tuple2<List<String>, List<String>>>("california",
                new Tuple2<List<String>, List<String>>(Arrays.asList("dodgers"), Arrays.asList("giants"))),
            new Tuple2<String, Tuple2<List<String>, List<String>>>("new york",
                new Tuple2<List<String>, List<String>>(Arrays.asList("yankees"), Arrays.asList("mets")))),
        Arrays.asList(
            new Tuple2<String, Tuple2<List<String>, List<String>>>("california",
                new Tuple2<List<String>, List<String>>(Arrays.asList("sharks"), Arrays.asList("ducks"))),
            new Tuple2<String, Tuple2<List<String>, List<String>>>("new york",
                new Tuple2<List<String>, List<String>>(Arrays.asList("rangers"), Arrays.asList("islanders")))));


    JavaDStream<Tuple2<String, String>> stream1 = JavaTestUtils.attachTestInputStream(
        sc, stringStringKVStream1, 1);
    JavaPairDStream<String, String> pairStream1 = JavaPairDStream.fromJavaDStream(stream1);

    JavaDStream<Tuple2<String, String>> stream2 = JavaTestUtils.attachTestInputStream(
        sc, stringStringKVStream2, 1);
    JavaPairDStream<String, String> pairStream2 = JavaPairDStream.fromJavaDStream(stream2);

    JavaPairDStream<String, Tuple2<List<String>, List<String>>> grouped = pairStream1.cogroup(pairStream2);
    JavaTestUtils.attachTestOutputStream(grouped);
    List<List<Tuple2<String, String>>> result = JavaTestUtils.runStreams(sc, 2, 2);

    Assert.assertEquals(expected, result);
  }

  @Test
  public void testJoin() {
    List<List<Tuple2<String, String>>> stringStringKVStream1 = Arrays.asList(
        Arrays.asList(new Tuple2<String, String>("california", "dodgers"),
            new Tuple2<String, String>("new york", "yankees")),
        Arrays.asList(new Tuple2<String, String>("california", "sharks"),
            new Tuple2<String, String>("new york", "rangers")));

    List<List<Tuple2<String, String>>> stringStringKVStream2 = Arrays.asList(
        Arrays.asList(new Tuple2<String, String>("california", "giants"),
            new Tuple2<String, String>("new york", "mets")),
        Arrays.asList(new Tuple2<String, String>("california", "ducks"),
            new Tuple2<String, String>("new york", "islanders")));


    List<List<Tuple2<String, Tuple2<String, String>>>> expected = Arrays.asList(
        Arrays.asList(
            new Tuple2<String, Tuple2<String, String>>("california",
                new Tuple2<String, String>("dodgers", "giants")),
            new Tuple2<String, Tuple2<String, String>>("new york",
                new Tuple2<String, String>("yankees", "mets"))),
        Arrays.asList(
            new Tuple2<String, Tuple2<String, String>>("california",
                new Tuple2<String, String>("sharks", "ducks")),
            new Tuple2<String, Tuple2<String, String>>("new york",
                new Tuple2<String, String>("rangers", "islanders"))));


    JavaDStream<Tuple2<String, String>> stream1 = JavaTestUtils.attachTestInputStream(
        sc, stringStringKVStream1, 1);
    JavaPairDStream<String, String> pairStream1 = JavaPairDStream.fromJavaDStream(stream1);

    JavaDStream<Tuple2<String, String>> stream2 = JavaTestUtils.attachTestInputStream(
        sc, stringStringKVStream2, 1);
    JavaPairDStream<String, String> pairStream2 = JavaPairDStream.fromJavaDStream(stream2);

    JavaPairDStream<String, Tuple2<String, String>> joined = pairStream1.join(pairStream2);
    JavaTestUtils.attachTestOutputStream(joined);
    List<List<Tuple2<String, Long>>> result = JavaTestUtils.runStreams(sc, 2, 2);

    Assert.assertEquals(expected, result);
  }

  // Input stream tests. These mostly just test that we can instantiate a given InputStream with
  // Java arguments and assign it to a JavaDStream without producing type errors. Testing of the
  // InputStream functionality is deferred to the existing Scala tests.
  @Test
  public void testKafkaStream() {
    HashMap<String, Integer> topics = Maps.newHashMap();
    HashMap<KafkaPartitionKey, Long> offsets = Maps.newHashMap();
    JavaDStream test1 = sc.kafkaStream("localhost", 12345, "group", topics);
    JavaDStream test2 = sc.kafkaStream("localhost", 12345, "group", topics, offsets);
    JavaDStream test3 = sc.kafkaStream("localhost", 12345, "group", topics, offsets,
      StorageLevel.MEMORY_AND_DISK());
  }

  @Test
  public void testNetworkTextStream() {
    JavaDStream test = sc.networkTextStream("localhost", 12345);
  }

  @Test
  public void testNetworkString() {
    class Converter extends Function<InputStream, Iterable<String>> {
      public Iterable<String> call(InputStream in) {
        BufferedReader reader = new BufferedReader(new InputStreamReader(in));
        List<String> out = new ArrayList<String>();
        try {
          while (true) {
            String line = reader.readLine();
            if (line == null) { break; }
            out.add(line);
          }
        } catch (IOException e) { }
        return out;
      }
    }

    JavaDStream test = sc.networkStream(
      "localhost",
      12345,
      new Converter(),
      StorageLevel.MEMORY_ONLY());
  }

  @Test
  public void testTextFileStream() {
    JavaDStream test = sc.textFileStream("/tmp/foo");
  }

  @Test
  public void testRawNetworkStream() {
    JavaDStream test = sc.rawNetworkStream("localhost", 12345);
  }

  @Test
  public void testFlumeStream() {
    JavaDStream test = sc.flumeStream("localhost", 12345);
  }

  @Test
  public void testFileStream() {
    JavaPairDStream<String, String> foo =
      sc.<String, String, SequenceFileInputFormat>fileStream("/tmp/foo");
  }
}
