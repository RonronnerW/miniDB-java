package com.wang.tx.recovery;

import com.wang.MiniDB;
import com.wang.log.BasicLogRecord;
import com.wang.log.LogManager;

/**
 * 静态检查点
 * 静态检查点日志记录就是日志文件中的一个标记记录而已。恢复管理器不需要检查静态检查点日志记录之前的所有日志记录。
 */
public class CheckpointRecord implements LogRecord {


    public CheckpointRecord() {

    }

    /**
     * 根据一条BasicLogRecord来构造一条CheckpointRecord。
     * 该构造函数是为了给 恢复/回滚 算法调用
     * <p>
     * 注意，一条提交日志记录的格式为：
     * <p>
     * <CHECKPOINT>
     *
     * @param blr
     */
    public CheckpointRecord(BasicLogRecord blr) {

    }

    @Override
    public int writeToLog() {
        Object[] rec = new Object[]{CHECKPOINT};
        LogManager logMgr = MiniDB.getLm();
        return logMgr.append(rec);
    }

    @Override
    public int op() {
        return CHECKPOINT;
    }

    /**
     * Checkpoint 日志记录没有对应的事务ID
     *
     * @return -1，a dummy value
     */
    @Override
    public int txNumber() {
        return -1;
    }

    @Override
    public void undo() {
        // empty is Okay
    }

    public String toString() {
        return "<CHECKPOINT>";
    }
}
