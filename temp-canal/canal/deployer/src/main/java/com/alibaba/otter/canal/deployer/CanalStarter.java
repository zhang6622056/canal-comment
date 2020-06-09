package com.alibaba.otter.canal.deployer;

import java.util.Properties;

import com.alibaba.otter.canal.connector.core.config.MQProperties;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.otter.canal.admin.netty.CanalAdminWithNetty;
import com.alibaba.otter.canal.connector.core.spi.CanalMQProducer;
import com.alibaba.otter.canal.connector.core.spi.ExtensionLoader;
import com.alibaba.otter.canal.deployer.admin.CanalAdminController;
import com.alibaba.otter.canal.server.CanalMQStarter;

/**
 * Canal server 启动类
 *
 * @author rewerma 2020-01-27
 * @version 1.0.2
 */
public class CanalStarter {

    private static final Logger logger                    = LoggerFactory.getLogger(CanalStarter.class);

    private static final String CONNECTOR_SPI_DIR         = "/plugin";
    private static final String CONNECTOR_STANDBY_SPI_DIR = "/canal/plugin";

    private CanalController     controller                = null;
    private CanalMQProducer     canalMQProducer           = null;
    private Thread              shutdownThread            = null;
    private CanalMQStarter      canalMQStarter            = null;
    //- canal.properties 配置文件
    private volatile Properties properties;
    private volatile boolean    running                   = false;

    private CanalAdminWithNetty canalAdmin;

    public CanalStarter(Properties properties){
        this.properties = properties;
    }

    public boolean isRunning() {
        return running;
    }

    public Properties getProperties() {
        return properties;
    }

    public void setProperties(Properties properties) {
        this.properties = properties;
    }

    public CanalController getController() {
        return controller;
    }





    
    /***
     * @Description: 如果servermode指定的是mq方式，则初始化producer
     * @Param: []
     * @return: void
     * @Author: zhanglei
     * @Date: 2020/6/9
     */
    private final void initServerModeBySpi(){
        //- 如果是mq生产相关消息，则使用spi的形式加载相关的实现
        //- CanalMQProducer是统一抽象，任何具体实现类都需要实现其方法，比如init方法
        String serverMode = CanalController.getProperty(properties, CanalConstants.CANAL_SERVER_MODE);
        if (!"tcp".equalsIgnoreCase(serverMode)) {
            ExtensionLoader<CanalMQProducer> loader = ExtensionLoader.getExtensionLoader(CanalMQProducer.class);
            canalMQProducer = loader
                    .getExtension(serverMode.toLowerCase(), CONNECTOR_SPI_DIR, CONNECTOR_STANDBY_SPI_DIR);
            if (canalMQProducer != null) {
                ClassLoader cl = Thread.currentThread().getContextClassLoader();
                Thread.currentThread().setContextClassLoader(canalMQProducer.getClass().getClassLoader());
                canalMQProducer.init(properties);
                Thread.currentThread().setContextClassLoader(cl);
            }
        }


        if (canalMQProducer != null) {
            MQProperties mqProperties = canalMQProducer.getMqProperties();
            //- 如果是mq mode，则禁用netty，因为此时并不需要客户端
            System.setProperty(CanalConstants.CANAL_WITHOUT_NETTY, "true");
            //- 是否分区，如果为true，则需要多个分区，false则为单一分区
            if (mqProperties.isFlatMessage()) {
                // 设置为raw避免ByteString->Entry的二次解析
                System.setProperty("canal.instance.memory.rawEntry", "false");
            }
        }
    }







    /***
     * TODO-ZL 分析stop和countDown
     * @Description: 使用Runtime，在关闭服务之前hook，调用stop服务
     * @Param: []
     * @return: void
     * @Author: zhanglei
     * @Date: 2020/6/9
     */
    private void addHookToShutdownGracefully(){
        shutdownThread = new Thread() {
            public void run() {
                try {
                    logger.info("## stop the canal server");
                    controller.stop();
                    CanalLauncher.runningLatch.countDown();
                } catch (Throwable e) {
                    logger.warn("##something goes wrong when stopping canal Server:", e);
                } finally {
                    logger.info("## canal server is down.");
                }
            }

        };
        Runtime.getRuntime().addShutdownHook(shutdownThread);
    }




    /****
     * TODO-ZL 等待分析
     * @Description: 执行CanalMqStart
     * @Param: []
     * @return: void
     * @Author: zhanglei
     * @Date: 2020/6/9
     */
    private final void CanalMQStarterDoStart(){
        if (canalMQProducer != null) {
            canalMQStarter = new CanalMQStarter(canalMQProducer);
            String destinations = CanalController.getProperty(properties, CanalConstants.CANAL_DESTINATIONS);
            canalMQStarter.start(destinations);
            controller.setCanalMQStarter(canalMQStarter);
        }
    }



    /****
     * TODO-ZL 待分析
     * @Description: 开始admin的Netty服务
     * @Param: []
     * @return: void
     * @Author: zhanglei
     * @Date: 2020/6/9
     */
    private final void CanalAdminWithNettyDoStart(){
        // start canalAdmin
        String port = CanalController.getProperty(properties, CanalConstants.CANAL_ADMIN_PORT);
        if (canalAdmin == null && StringUtils.isNotEmpty(port)) {
            String user = CanalController.getProperty(properties, CanalConstants.CANAL_ADMIN_USER);
            String passwd = CanalController.getProperty(properties, CanalConstants.CANAL_ADMIN_PASSWD);
            CanalAdminController canalAdmin = new CanalAdminController(this);
            canalAdmin.setUser(user);
            canalAdmin.setPasswd(passwd);

            String ip = CanalController.getProperty(properties, CanalConstants.CANAL_IP);

            logger.debug("canal admin port:{}, canal admin user:{}, canal admin password: {}, canal ip:{}",
                    port,
                    user,
                    passwd,
                    ip);

            CanalAdminWithNetty canalAdminWithNetty = CanalAdminWithNetty.instance();
            canalAdminWithNetty.setCanalAdmin(canalAdmin);
            canalAdminWithNetty.setPort(Integer.parseInt(port));
            canalAdminWithNetty.setIp(ip);
            canalAdminWithNetty.start();
            this.canalAdmin = canalAdminWithNetty;
        }
    }





    /**
     * 启动方法
     *
     * @throws Throwable
     */
    public synchronized void start() throws Throwable {
        //- 初始化servermode，如果servermode指定的是mq方式，则初始化producer
        initServerModeBySpi();

        //- 启动canalController服务
        logger.info("## start the canal server.");
        controller = new CanalController(properties);
        controller.start();
        logger.info("## the canal server is running now ......");

        //- 优雅的关闭服务
        addHookToShutdownGracefully();
        //- 启动mq的服务
        CanalMQStarterDoStart();
        //- 启动admin的Netty服务
        CanalAdminWithNettyDoStart();
        running = true;
    }

    public synchronized void stop() throws Throwable {
        stop(false);
    }

    /**
     * 销毁方法，远程配置变更时调用
     *
     * @throws Throwable
     */
    public synchronized void stop(boolean stopByAdmin) throws Throwable {
        if (!stopByAdmin && canalAdmin != null) {
            canalAdmin.stop();
            canalAdmin = null;
        }

        if (controller != null) {
            controller.stop();
            controller = null;
        }
        if (shutdownThread != null) {
            Runtime.getRuntime().removeShutdownHook(shutdownThread);
            shutdownThread = null;
        }
        if (canalMQProducer != null && canalMQStarter != null) {
            canalMQStarter.destroy();
            canalMQStarter = null;
            canalMQProducer = null;
        }
        running = false;
    }
}
