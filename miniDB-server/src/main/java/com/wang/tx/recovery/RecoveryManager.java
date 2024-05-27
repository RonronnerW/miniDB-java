package com.wang.tx.recovery;

import com.sun.source.tree.WhileLoopTree;
import com.wang.MiniDB;
import com.wang.buffer.Buffer;
import com.wang.file.Block;

import java.util.ArrayList;

/**
 * 恢复管理器
 * 每个事务都会创建自己的恢复管理器
 */
public class RecoveryManager {

    private int txNum;

    /**
     * 构造器会写一条开始日志记录到日志文件
     *
     * @param txNum
     */
    public RecoveryManager(int txNum) {
        this.txNum = txNum;
        new StartRecord(txNum).writeToLog();
    }

    /**
     * 提交
     * Undo-only恢复下的事务提交算法
     * <p>
     * 1. 将事务修改的页中的内容flush到磁盘上。
     * 2. 写一条commit log record。
     * 3. 将包含日志记录的日志页flush到日志文件上。
     */
    public void commit() {
        MiniDB.getBm().flushAll(txNum);
        int lsn = new CommitRecord(txNum).writeToLog();
        MiniDB.getLm().flush(lsn);
    }

    /**
     * 回滚
     */
    public void rollback() throws InterruptedException {
        MiniDB.getBm().flushAll(txNum);
        doRollback();  // 把修改后的值，再改回来
        // 为什么rollback之后还要flushAll呢？
        // 这是因为，更新日志记录在undo过程中也会修改相应的缓冲区，因此需要flush到磁盘
        MiniDB.getBm().flushAll(txNum);
        int lsn = new RollBackRecord(txNum).writeToLog();
        MiniDB.getLm().flush(lsn);
    }

    /**
     * 回滚操作
     * 该方法会遍历日志记录，调用遍历到的每条日志记录的undo()方法，
     * 直到该事务的START日志记录为止。
     */
    private void doRollback() throws InterruptedException {
        LogRecordIterator iterator = new LogRecordIterator();
        while (iterator.hasNext()) {
            LogRecord logRecord = iterator.next();
            if (logRecord.txNumber() == txNum) {
                if (logRecord.op() == LogRecord.START) {
                    return;
                } else {
                    // 其实只有SetIntRecord和SetStringRecord
                    // 的undo()方法才有具体的实现，其他日志记录类
                    // 的undo()方法都是空方法。
                    logRecord.undo();// 回滚
                }
            }
        }
    }

    /**
     * 恢复
     */
    public void recover() throws InterruptedException {
        MiniDB.getBm().flushAll(txNum);
        doRecover();
        MiniDB.getBm().flushAll(txNum);
        int lsn = new CheckpointRecord().writeToLog();
        MiniDB.getLm().flush(lsn);
    }

    /**
     * 恢复操作
     * 该方法会遍历日志记录，无论何时它发现一个未完成事务的日志记录，
     * 它都会调用该日志记录的undo()方法。
     * 当遇到一个CHECKPOINT日志记录或日志文件尾时（从后往前读，所以实际上是文件头），恢复算法停止。
     */
    private void doRecover() throws InterruptedException {
        ArrayList<Integer> txIds = new ArrayList<>(); // 已经提交或回滚的事务id
        LogRecordIterator iterator = new LogRecordIterator();
        while (iterator.hasNext()) {
            LogRecord logRecord = iterator.next();
            if (logRecord.op() == LogRecord.CHECKPOINT) {
                return;
            } else if (logRecord.op() == LogRecord.COMMIT || logRecord.op() == LogRecord.ROLLBACK) {
                txIds.add(logRecord.txNumber());
            } else if (!txIds.contains(logRecord.txNumber())) {
                logRecord.undo();
            }

        }
    }

    /**
     * 写一条SetInt日志记录到日志文件, 并返回其LSN
     *
     * @param buffer
     * @param offset
     * @param newVal
     * @return
     */
    public int setInt(Buffer buffer, int offset, int newVal) {
        int oldVal = buffer.getInt(offset);
        Block blk = buffer.block();

        if (isTemporaryBlock(blk))
            return -1;
        else
            return new SetIntRecord(txNum, blk, offset, oldVal).writeToLog();
    }

    /**
     * 更新
     *
     * @param buffer
     * @param offset
     * @param newStr
     * @return
     */
    public int setString(Buffer buffer, int offset, String newStr) {
        String oldVal = buffer.getString(offset);
        Block blk = buffer.block();

        if (isTemporaryBlock(blk))
            return -1;
        else
            return new SetStringRecord(txNum, blk, offset, oldVal).writeToLog();
    }

    private boolean isTemporaryBlock(Block blk) {
        return blk.getFileName().startsWith("temp");
    }
}