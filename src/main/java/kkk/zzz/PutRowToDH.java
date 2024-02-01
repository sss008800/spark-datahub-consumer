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
 * @描述  往datahub的topic插入流数据，生产测试数据
 */
public class PutRowToDH {
    public static DatahubClient client;

    public static void init() {
        System.out.println("====================================================>>>>>>>>>>>>>>>>>>>>>>>>");
        DatahubClient client3 = DatahubClientBuilder.newBuilder()
                .setDatahubConfig(
                        new DatahubConfig("https://dh-cn-hangzhou.aliyuncs.com", new AliyunAccount("accessId", "accessKey"))
                ).build();
        client = client3;
    }

    public static void main(String[] args) {
        init();
        //createTopic();
        putRecords();
//        consumer();
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

}
