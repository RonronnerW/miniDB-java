package com.wang.tx.recovery;

public interface LogRecord {
    /**
     * 日志记录的第一个值：标记操作类型的整数
     */
    static final int CHECKPOINT = 0, START = 1,
            COMMIT = 2, ROLLBACK = 3,
            SETINT = 4, SETSTRING = 5;

    /**
     * 将记录追加到日志并返回LSN
     * @return
     */
    int writeToLog();

    /**
     * 返回记录的操作类型
     * @return
     */
    int op();

    /**
     * 返回事务id
     * @return
     */
    int txNumber();

    /**
     * 还原存储在该记录中的所有更改
     */
    void undo() throws InterruptedException;
}