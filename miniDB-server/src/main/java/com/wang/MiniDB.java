package com.wang;

import com.wang.file.FileManager;
import com.wang.log.LogManager;
import java.io.IOException;

public class MiniDB {
    private static FileManager fm;
    private static LogManager lm;

    public static FileManager getFm() {
        return fm;
    }

    public static LogManager getLm() {
        return lm;
    }

    /**
     * 初始化数据库
     */
    public void init() {
        // 初始化文件管理器
        initFileManager("/miniDB");
        // 初始化日志管理器
        initLogManager("/miniDB.log");
    }

    /**
     * 初始化文件管理器
     *
     * @param dirname 数据库文件夹名
     */
    private void initFileManager(String dirname) {
        fm = new FileManager(dirname);
    }

    /**
     * 初始化日志管理器
     *
     * @param logFileName 日志文件名
     */
    private void initLogManager(String logFileName) {
        try {
            lm = new LogManager(logFileName);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
