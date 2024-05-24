package com.wang;

import com.wang.buffer.BasicBufferManager;
import com.wang.file.FileManager;
import com.wang.log.LogManager;
import java.io.IOException;

public class MiniDB {
    private static FileManager fm;
    private static LogManager lm;
    private static BasicBufferManager bm;

    public static FileManager getFm() {
        return fm;
    }

    public static LogManager getLm() {
        return lm;
    }
    public static BasicBufferManager getBm() {
        return bm;
    }

    /**
     * 初始化数据库
     */
    public static void init() {
        // 初始化文件管理器
        initFileManager("/miniDB");
        // 初始化日志管理器
        initLogManager("/miniDB.log");
        // 初始化缓冲管理器
        initBufferManager(4);
    }

    /**
     * 初始化缓冲管理器
     * @param bufferPoolSize 缓冲池大小
     */
    private static void initBufferManager(int bufferPoolSize) {
        bm = new BasicBufferManager(4);
    }

    /**
     * 初始化文件管理器
     *
     * @param dirname 数据库文件夹名
     */
    private static void initFileManager(String dirname) {
        fm = new FileManager(dirname);
    }

    /**
     * 初始化日志管理器
     *
     * @param logFileName 日志文件名
     */
    private static void initLogManager(String logFileName) {
        try {
            lm = new LogManager(logFileName);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
