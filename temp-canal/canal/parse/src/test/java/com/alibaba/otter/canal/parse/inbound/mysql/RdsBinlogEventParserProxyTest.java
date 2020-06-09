package com.alibaba.otter.canal.parse.inbound.mysql;

import java.net.InetSocketAddress;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import com.alibaba.otter.canal.common.alarm.LogAlarmHandler;
import com.alibaba.otter.canal.common.zookeeper.ZkClientx;
import com.alibaba.otter.canal.meta.PeriodMixedMetaManager;
import com.alibaba.otter.canal.meta.ZooKeeperMetaManager;
import com.alibaba.otter.canal.parse.ha.HeartBeatHAController;
import com.alibaba.otter.canal.parse.index.FailbackLogPositionManager;
import com.alibaba.otter.canal.parse.index.MemoryLogPositionManager;
import com.alibaba.otter.canal.parse.index.MetaLogPositionManager;
import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import com.alibaba.otter.canal.filter.aviater.AviaterRegexFilter;
import com.alibaba.otter.canal.parse.helper.TimeoutChecker;
import com.alibaba.otter.canal.parse.inbound.mysql.rds.RdsBinlogEventParserProxy;
import com.alibaba.otter.canal.parse.index.AbstractLogPositionManager;
import com.alibaba.otter.canal.parse.stub.AbstractCanalEventSinkTest;
import com.alibaba.otter.canal.parse.support.AuthenticationInfo;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.position.EntryPosition;
import com.alibaba.otter.canal.protocol.position.LogPosition;
import com.alibaba.otter.canal.sink.exception.CanalSinkException;
import org.springframework.beans.factory.config.MethodInvokingFactoryBean;

/**
 * @author chengjin.lyf on 2018/7/21 下午5:24
 * @since 1.0.25
 */
@Ignore
public class RdsBinlogEventParserProxyTest {

    private static final String DETECTING_SQL = "insert into retl.xdual values(1,now()) on duplicate key update x=now()";
    private static final String MYSQL_ADDRESS = "";
    private static final String USERNAME      = "";
    private static final String PASSWORD      = "";
    public static final String  DBNAME        = "";
    public static final String  TBNAME        = "";
    public static final String  DDL           = "";

    @Test
    public void test_timestamp() throws InterruptedException {
        final TimeoutChecker timeoutChecker = new TimeoutChecker(3000 * 1000);
        final AtomicLong entryCount = new AtomicLong(0);
        final EntryPosition entryPosition = new EntryPosition();

        final RdsBinlogEventParserProxy controller = new RdsBinlogEventParserProxy();
        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.HOUR_OF_DAY, -24 * 4);
        final EntryPosition defaultPosition = buildPosition(null, null, calendar.getTimeInMillis());
        controller.setSlaveId(3344L);
        controller.setDetectingEnable(false);
        controller.setDetectingSQL(DETECTING_SQL);
        controller.setMasterInfo(buildAuthentication());
        controller.setMasterPosition(defaultPosition);
        controller.setInstanceId("");
        controller.setAccesskey("");
        controller.setSecretkey("");
        controller.setDirectory("/tmp/binlog");
        controller.setEventBlackFilter(new AviaterRegexFilter("mysql\\.*"));
        controller.setFilterTableError(true);
        controller.setBatchFileSize(4);
        controller.setEventSink(new AbstractCanalEventSinkTest<List<CanalEntry.Entry>>() {

            @Override
            public boolean sink(List<CanalEntry.Entry> entrys, InetSocketAddress remoteAddress, String destination)
                                                                                                                   throws CanalSinkException {
                for (CanalEntry.Entry entry : entrys) {
                    if (entry.getEntryType() != CanalEntry.EntryType.HEARTBEAT) {
                        entryCount.incrementAndGet();

                        String logfilename = entry.getHeader().getLogfileName();
                        long logfileoffset = entry.getHeader().getLogfileOffset();
                        long executeTime = entry.getHeader().getExecuteTime();

                        entryPosition.setJournalName(logfilename);
                        entryPosition.setPosition(logfileoffset);
                        entryPosition.setTimestamp(executeTime);
                        break;
                    }
                }
                return true;
            }
        });

        controller.setLogPositionManager(new AbstractLogPositionManager() {

            private LogPosition logPosition;

            public void persistLogPosition(String destination, LogPosition logPosition) {
                this.logPosition = logPosition;
            }

            public LogPosition getLatestIndexBy(String destination) {
                return logPosition;
            }
        });

        controller.start();
        timeoutChecker.waitForIdle();

        if (controller.isStart()) {
            controller.stop();
        }

        // check
        Assert.assertTrue(entryCount.get() > 0);

        // 对比第一条数据和起始的position相同
        Assert.assertEquals(entryPosition.getJournalName(), "mysql-bin.000001");
        Assert.assertTrue(entryPosition.getPosition() <= 6163L);
        Assert.assertTrue(entryPosition.getTimestamp() <= defaultPosition.getTimestamp());
    }

    // ======================== helper method =======================

    private EntryPosition buildPosition(String binlogFile, Long offest, Long timestamp) {
        return new EntryPosition(binlogFile, offest, timestamp);
    }

    private AuthenticationInfo buildAuthentication() {
        return new AuthenticationInfo(new InetSocketAddress(MYSQL_ADDRESS, 3306), USERNAME, PASSWORD);
    }


    @Test
    public void testLocalMaster() throws Exception {
        final TimeoutChecker timeoutChecker = new TimeoutChecker(3 * 1000);
        final AtomicLong entryCount = new AtomicLong(0);
        final EntryPosition entryPosition = new EntryPosition();

        final RdsBinlogEventParserProxy binlogEventParserProxy = new RdsBinlogEventParserProxy();
        binlogEventParserProxy.setSlaveId(3344L);
        binlogEventParserProxy.setDestination("ocs_test");

        //- 心跳相关
        binlogEventParserProxy.setDetectingEnable(true);
        binlogEventParserProxy.setDetectingSQL("select 1");
        binlogEventParserProxy.setDetectingIntervalInSeconds(5);

        //- 心跳检测3次失败，自动切换
        HeartBeatHAController heartBeatHAController = new HeartBeatHAController();
        heartBeatHAController.setDetectingRetryTimes(3);
        heartBeatHAController.setSwitchEnable(true);
        binlogEventParserProxy.setHaController(heartBeatHAController);

        //- TODO-ZL
        LogAlarmHandler logAlarmHandler = new LogAlarmHandler();
        binlogEventParserProxy.setAlarmHandler(logAlarmHandler);

        //- 解析过滤，只解析匹配相关表
        binlogEventParserProxy.setEventFilter(new AviaterRegexFilter("o2o_ocs_core\\\\.order_.*"));
        //- 排除解析
        binlogEventParserProxy.setEventBlackFilter(new AviaterRegexFilter("",false));

        //- 最大事务解析大小，超过该大小后事务将被切分为多个事务投递
        binlogEventParserProxy.setTransactionSize(1024);

        //- 网络连接相关参数
        binlogEventParserProxy.setReceiveBufferSize(16384);
        binlogEventParserProxy.setSendBufferSize(16384);
        binlogEventParserProxy.setDefaultConnectionTimeoutInSeconds(30);

        //- 解析编码
        binlogEventParserProxy.setConnectionCharsetNumber((byte)33);
        binlogEventParserProxy.setConnectionCharset("UTF-8");



        //- 初始化zookeeper客户端
//        MethodInvokingFactoryBean methodInvokingFactoryBean = new MethodInvokingFactoryBean();
//        methodInvokingFactoryBean.setTargetClass(ZkClientx.class);
//        methodInvokingFactoryBean.setTargetMethod("getZkClient");
//        methodInvokingFactoryBean.setArguments(new Object[]{"127.0.0.1:2181"});

        ZkClientx zkClientx = new ZkClientx("127.0.0.1:2181");

        //- 消费位点
        ZooKeeperMetaManager zooKeeperMetaManager = new ZooKeeperMetaManager();
        zooKeeperMetaManager.setZkClientx(zkClientx);
        PeriodMixedMetaManager periodMixedMetaManager = new PeriodMixedMetaManager();
        periodMixedMetaManager.setZooKeeperMetaManager(zooKeeperMetaManager);
        FailbackLogPositionManager failbackLogPositionManager = new FailbackLogPositionManager(new MemoryLogPositionManager(),new MetaLogPositionManager(periodMixedMetaManager));
        binlogEventParserProxy.setLogPositionManager(failbackLogPositionManager);

        //-# canal发生mysql切换时，在新的mysql库上查找binlog时需要往前查找的时间，单位秒
        //# 说明：mysql主备库可能存在解析延迟或者时钟不统一，需要回退一段时间，保证数据不丢
        binlogEventParserProxy.setFallbackIntervalInSeconds(60);

        //- mysql master信息
        AuthenticationInfo authenticationInfo = new AuthenticationInfo(new InetSocketAddress("127.0.0.1", 3306), "canal", "canal");
        authenticationInfo.setDefaultDatabaseName("o2o_ocs_core");
        authenticationInfo.initPwd();
        binlogEventParserProxy.setMasterInfo(authenticationInfo);

        //- 指定从某个位置开始解析
        EntryPosition masterPosition = new EntryPosition();
        binlogEventParserProxy.setMasterPosition(masterPosition);



        //- # 是否忽略dcl语句，比如grant/create user等 false为不忽略，true为忽略
        binlogEventParserProxy.setFilterQueryDcl(false);
        //- # 是否忽略DDL的query语句，比如create table/alater table/drop table/rename table/create index/drop index. false为不忽略，true为忽略
        binlogEventParserProxy.setFilterQueryDdl(false);
        //- # 是否忽略DML的query语句，比如insert/update/delete table.(mysql5.6的ROW模式可以包含statement模式的query记录) false为不忽略，true为忽略
        binlogEventParserProxy.setFilterQueryDml(false);


        binlogEventParserProxy.setUseDruidDdlFilter(true);
        //- # 用于仅订阅除rows以外的数据，行数据将会被过滤 false为不忽略，true为忽略
        binlogEventParserProxy.setFilterRows(false);
        //-
        binlogEventParserProxy.setFilterTableError(false);
        binlogEventParserProxy.setSupportBinlogFormats("ROW,STATEMENT,MIXED");
        binlogEventParserProxy.setSupportBinlogImages("FULL,MINIMAL,NOBLOB");

        binlogEventParserProxy.setEnableTsdb(true);
        binlogEventParserProxy.setTsdbSpringXml("classpath:spring/tsdb/h2-tsdb.xml");
        binlogEventParserProxy.setTsdbSnapshotExpire(360);
        binlogEventParserProxy.setTsdbSnapshotInterval(24);


        binlogEventParserProxy.setIsGTIDMode(false);

        binlogEventParserProxy.setParallel(true);
        binlogEventParserProxy.setParallelBufferSize(16);
        binlogEventParserProxy.setParallelThreadSize(256);


        //TODO-ZL 暂不指定，默认运行
        EntryPosition defaultPosition = new EntryPosition();
        binlogEventParserProxy.setMasterPosition(defaultPosition);
        binlogEventParserProxy.start();




//        		<property name="standbyInfo">
//			<bean class="com.alibaba.otter.canal.parse.support.AuthenticationInfo" init-method="initPwd">
//				<property name="address" value="${canal.instance.standby.address}" />
//				<property name="username" value="${canal.instance.dbUsername:retl}" />
//				<property name="password" value="${canal.instance.dbPassword:retl}" />
//				<property name="pwdPublicKey" value="${canal.instance.pwdPublicKey:retl}" />
//				<property name="enableDruid" value="${canal.instance.enableDruid:false}" />
//				<property name="defaultDatabaseName" value="${canal.instance.defaultDatabaseName:}" />
//			</bean>
//		</property>
//        		<property name="standbyPosition">
//			<bean class="com.alibaba.otter.canal.protocol.position.EntryPosition">
//				<property name="journalName" value="${canal.instance.standby.journal.name}" />
//				<property name="position" value="${canal.instance.standby.position}" />
//				<property name="timestamp" value="${canal.instance.standby.timestamp}" />
//				<property name="gtid" value="${canal.instance.standby.gtid}" />
//			</bean>
//		</property>
















    }

}
