package kkk.zzz;

import com.aliyun.datahub.client.DatahubClient;
import com.aliyun.datahub.client.DatahubClientBuilder;
import com.aliyun.datahub.client.auth.AliyunAccount;
import com.aliyun.datahub.client.common.DatahubConfig;
import com.aliyun.datahub.client.exception.*;
import com.aliyun.datahub.client.model.*;
import org.apache.commons.collections.CollectionUtils;
import scala.Tuple2;
import scala.collection.JavaConverters;
import scala.collection.mutable.Seq;

import java.util.*;
import java.util.stream.Collectors;

/**
 * @创建人 dingxl
 * @创建时间 2024/1/4
 * @描述  消费kafka数据
 */
public class DatahubJavaDemo {
    public static DatahubClient client;

    public static void init() {
        System.out.println("====================================================>>>>>>>>>>>>>>>>>>>>>>>>");
        DatahubClient client3 = DatahubClientBuilder.newBuilder()
                .setDatahubConfig(
                        new DatahubConfig("https://dh-cn-hangzhou.aliyuncs.com", new AliyunAccount("LTA****", "srTEQf*****"))
                ).build();
        client = client3;
    }

    public static void getDStream() {

        init();
        //createTopic();
        //putRecords();
        consumer();
    }

    public static void createTopic() {
        //////////////////////////////// CreateTopic
        int shardCount = 3;
        int lifecycle = 7;
        RecordType type = RecordType.TUPLE;
        RecordSchema schema = new RecordSchema();
        schema.addField(new Field("field1", FieldType.BIGINT));
        schema.addField(new Field("field2", FieldType.STRING));
        String comment = "The first topic";
        client.createTopic("dxl_datahub", "topic_from_java_create", shardCount, lifecycle, type, schema, comment);
    }

    public static void putRecords() {
        String shardId = "1";

        RecordSchema recordSchema = client.getTopic("dxl_datahub", "topic_from_java_create").getRecordSchema();

        // 生成十条数据
        List<RecordEntry> recordEntries = new ArrayList<>();
        for (int i = 0; i < 10; ++i) {
            RecordEntry recordEntry = new RecordEntry();
            // 对每条数据设置额外属性
            recordEntry.addAttribute("key1", "value11");
            TupleRecordData data = new TupleRecordData(recordSchema);
            data.setField("field1", 1234567);
            data.setField("field2", "lshualm");
            recordEntry.setRecordData(data);
            recordEntry.setShardId(shardId);
            recordEntries.add(recordEntry);
        }

        int retryNum = 0;
        while (retryNum < 3) {
            try {
                // 服务端从2.12版本开始支持，之前版本请使用putRecords接口
                //datahubClient.putRecordsByShard(Constant.projectName, Constant.topicName, shardId, recordEntries);
                PutRecordsResult putRecordsResult = client.putRecords("dxl_datahub", "topic_from_java_create", recordEntries);
                System.out.println("write tuple data successful");
                System.out.println(putRecordsResult.getPutErrorEntries());
                break;
            } catch (InvalidParameterException e) {
                // invalid parameter
                e.printStackTrace();
                throw e;
            } catch (AuthorizationFailureException e) {
                // AK error
                e.printStackTrace();
                throw e;
            } catch (ResourceNotFoundException e) {
                // project or topic not found
                e.printStackTrace();
                throw e;
            } catch (ShardSealedException e) {
                // shard status is CLOSED, read only
                e.printStackTrace();
                throw e;
            } catch (LimitExceededException e) {
                // limit exceed, retry
                e.printStackTrace();
                retryNum++;
            } catch (DatahubClientException e) {
                // other error
                e.printStackTrace();
                retryNum++;
            }
        }
    }

    public static void consumer() {
        System.out.println("============================================<<<========>>>>>>>>>>>>>>>>>>>>>>>>");
        //点位消费示例，并在消费过程中进行点位的提交
        String shardId = "1";
        List<String> shardIds = Arrays.asList("0", "1");
        RecordSchema recordSchema = client.getTopic("dxl_datahub", "topic_from_java_create").getRecordSchema();

        OpenSubscriptionSessionResult openSubscriptionSessionResult = client.openSubscriptionSession("dxl_datahub", "topic_from_java_create", "170433772222372MOP", shardIds);
        SubscriptionOffset subscriptionOffset = openSubscriptionSessionResult.getOffsets().get(shardId);
        // 1、获取当前点位的cursor，如果当前点位已过期则获取生命周期内第一条record的cursor，未消费同样获取生命周期内第一条record的cursor
        String cursor = null;
        //sequence < 0说明未消费
        if (subscriptionOffset.getSequence() < 0) {
            // 获取生命周期内第一条record的cursor
            cursor = client.getCursor("dxl_datahub", "topic_from_java_create", shardId, CursorType.OLDEST).getCursor();
        } else {
            // 获取下一条记录的Cursor
            long nextSequence = subscriptionOffset.getSequence() + 1;
            try {
                //按照SEQUENCE getCursor可能报SeekOutOfRange错误，表示当前cursor的数据已过期
                cursor = client.getCursor("dxl_datahub", "topic_from_java_create", shardId, CursorType.SEQUENCE, nextSequence).getCursor();
            } catch (SeekOutOfRangeException e) {
                // 获取生命周期内第一条record的cursor
                cursor = client.getCursor("dxl_datahub", "topic_from_java_create", shardId, CursorType.OLDEST).getCursor();
            }
        }
        // 2、读取并保存点位，这里以读取Tuple数据为例，并且每1000条记录保存一次点位
        long recordCount = 0L;
        // 每次读取10条record
        int fetchNum = 10;
        while (true) {
            try {
                GetRecordsResult getRecordsResult = client.getRecords("dxl_datahub", "topic_from_java_create", shardId, recordSchema, cursor, fetchNum);
                if (getRecordsResult.getRecordCount() <= 0) {
                    // 无数据，sleep后读取
                    Thread.sleep(1000);
                    continue;
                }

                // list data
                List<Map<String,String>> dList = new ArrayList();

                for (RecordEntry recordEntry : getRecordsResult.getRecords()) {
                    //消费数据
                    TupleRecordData data = (TupleRecordData) recordEntry.getRecordData();
                    System.out.println("field1:" + data.getField("field1") + "\t"
                            + "field2:" + data.getField("field2"));

                    Map kv = new HashMap();
                    kv.put("field1", data.getField("field1").toString());
                    kv.put("field2", data.getField("field2").toString());
                    dList.add(kv);

                    // 处理数据完成后，设置点位
                    ++recordCount;
                    subscriptionOffset.setSequence(recordEntry.getSequence());
                    subscriptionOffset.setTimestamp(recordEntry.getSystemTime());
                    if (recordCount % 1000 == 0) {
                        //提交点位
                        Map<String, SubscriptionOffset> offsetMap = new HashMap<>();
                        offsetMap.put(shardId, subscriptionOffset);
                        client.commitSubscriptionOffset("dxl_datahub", "topic_from_java_create", "170433772222372MOP", offsetMap);
                        System.out.println("commit offset successful");
                    }
                }

                if (! CollectionUtils.isEmpty(dList)) {
                    // spark...
                    Seq<Tuple2<String, String>> scalaSeq = (Seq<Tuple2<String, String>>) JavaConverters.asScalaBufferConverter(
                            dList.stream()
                                    .map(v -> new Tuple2<>(v.get("field1").toString(), v.get("field2").toString())) // 假设每个String数组包含两个元素
                                    .collect(Collectors.toList())
                    ).asScala();
                    // save hive
                    sparkSave.saveToHive(scalaSeq);
                }

                cursor = getRecordsResult.getNextCursor();
            } catch (SubscriptionOfflineException | SubscriptionSessionInvalidException e) {
                // 退出. Offline: 订阅下线; SubscriptionSessionInvalid: 表示订阅被其他客户端同时消费
                break;
            } catch (SubscriptionOffsetResetException e) {
                // 表示点位被重置，重新获取SubscriptionOffset信息，这里以Sequence重置为例
                // 如果以Timestamp重置，需要通过CursorType.SYSTEM_TIME获取cursor
                subscriptionOffset = client.getSubscriptionOffset("dxl_datahub", "topic_from_java_create", "170433772222372MOP", shardIds).getOffsets().get(shardId);
                long nextSequence = subscriptionOffset.getSequence() + 1;
                cursor = client.getCursor("dxl_datahub", "topic_from_java_create", shardId, CursorType.SEQUENCE, nextSequence).getCursor();
            } catch (DatahubClientException e) {
                // TODO: 针对不同异常决定是否退出
            } catch (Exception e) {
                System.out.println(e);
                //java.lang.ClassCastException:
                // scala.collection.convert.Wrappers$JListWrapper cannot be cast to scala.collection.immutable.Seq
                break;
            }
        }
    }
}
