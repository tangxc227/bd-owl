package com.owl.javasdk.sdk;

import com.owl.javasdk.util.HttpRequestUtil;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @Author: tangxc
 * @Description:
 * @Date: Created in 10:56 2018/12/19
 * @Modified by:
 */
public class SendDataMonitor {

    private static final Logger LOGGER = Logger.getGlobal();

    private BlockingQueue<String> queue = new LinkedBlockingDeque<>();

    private static SendDataMonitor instance = null;

    private SendDataMonitor() {

    }

    public static SendDataMonitor getInstance() {
        if (null == instance) {
            synchronized (SendDataMonitor.class) {
                if (null == instance) {
                    instance = new SendDataMonitor();
                    Thread thread = new Thread(new Runnable() {
                        @Override
                        public void run() {
                            SendDataMonitor.instance.run();
                        }
                    });
                    thread.setDaemon(true);
                    thread.start();
                }
            }
        }
        return instance;
    }

    public void run() {
        while (true) {
            try {
                String url = this.queue.take();
                HttpRequestUtil.sendGet(url);
            } catch (InterruptedException e) {
                LOGGER.log(Level.WARNING, "发送url异常", e);
            }
        }
    }

    public static void addSendUrl(String url) throws InterruptedException {
        getInstance().queue.put(url);
    }

}
