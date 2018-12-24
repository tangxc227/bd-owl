package com.owl.trans.etl.mr.ald;

import com.owl.trans.common.EventEnum;
import com.owl.trans.common.EventLogConstants;
import com.owl.trans.etl.util.LoggerUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Map;
import java.util.zip.CRC32;

/**
 * @Author: tangxc
 * @Description:
 * @Date: Created in 11:07 2018/12/24
 * @Modified by:
 */
public class AnalyserLogDataMapper extends Mapper<Object, Text, NullWritable, Put> {

    private static final Logger LOGGER = Logger.getLogger(AnalyserLogDataMapper.class);

    // 主要用于标志，方便查看过滤数据
    private int inputRecords, filterRecords, outputRecords;

    private byte[] family = Bytes.toBytes(EventLogConstants.EVENT_LOGS_FAMILY_NAME);
    private CRC32 crc32 = new CRC32();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        this.inputRecords++;
        LOGGER.debug("处理数据：" + value.toString());

        try {
            // 解析日志
            Map<String, String> clientInfo = LoggerUtil.handleLog(value.toString());
            if (clientInfo.isEmpty()) {
                this.filterRecords++;
                return;
            }
            String eventAliasName = clientInfo.get(EventLogConstants.LOG_COLUMN_NAME_EVENT_NAME);
            EventEnum eventEnum = EventEnum.valueOfAlias(eventAliasName);
            switch (eventEnum) {
                case LAUNCH:
                case PAGEVIEW:
                case CHARGEREQUEST:
                case CHARGESUCCESS:
                case CHARGEREFUND:
                case EVENT:
                    // 处理数据
                    this.handleData(clientInfo, eventEnum, context);
                    break;
                default:
                    this.filterRecords++;
                    LOGGER.warn("该事件没法进行解析，事件名称为:" + eventAliasName);
            }
        } catch (Exception e) {
            this.filterRecords++;
            LOGGER.error("处理数据发生异常， 数据：" + value, e);
        }

    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
        LOGGER.info("输入数据：" + this.inputRecords + "；输出数据：" + this.outputRecords + "；过滤数据：" + this.filterRecords);
    }

    private void handleData(Map<String, String> clientInfo, EventEnum eventEnum, Context context) throws IOException, InterruptedException {
        String uuid = clientInfo.get(EventLogConstants.LOG_COLUMN_NAME_UUID);
        String memberId = clientInfo.get(EventLogConstants.LOG_COLUMN_NAME_MEMBER_ID);
        String serverTime = clientInfo.get(EventLogConstants.LOG_COLUMN_NAME_SERVER_TIME);
        if (StringUtils.isNotBlank(serverTime)) {
            // 去掉浏览器信息
            clientInfo.remove(EventLogConstants.LOG_COLUMN_NAME_USER_AGENT);
            String rowKey = this.generateRowKey(uuid, memberId, eventEnum.alias, serverTime);
            Put put = new Put(Bytes.toBytes(rowKey));
            for (Map.Entry<String, String> entry : clientInfo.entrySet()) {
                if (StringUtils.isNotBlank(entry.getKey()) && StringUtils.isNotBlank(entry.getValue())) {
                    put.add(family, Bytes.toBytes(entry.getKey()), Bytes.toBytes(entry.getValue()));
                }
            }
            context.write(NullWritable.get(), put);
            this.outputRecords++;
        } else {
            this.filterRecords++;
        }
    }

    /**
     * 根据uuid memberid servertime创建rowkey
     *
     * @param uuid
     * @param memberId
     * @param alias
     * @param serverTime
     * @return
     */
    private String generateRowKey(String uuid, String memberId, String alias, String serverTime) {
        StringBuilder builder = new StringBuilder();
        builder.append(serverTime).append("_");
        this.crc32.reset();
        if (StringUtils.isNotBlank(uuid)) {
            this.crc32.update(uuid.getBytes());
        }
        if (StringUtils.isNotBlank(memberId)) {
            this.crc32.update(memberId.getBytes());
        }
        this.crc32.update(alias.getBytes());
        builder.append(this.crc32.getValue() % 100000000L);
        return builder.toString();
    }

}
