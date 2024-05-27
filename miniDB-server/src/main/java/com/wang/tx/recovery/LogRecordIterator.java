package com.wang.tx.recovery;

import com.wang.MiniDB;
import com.wang.log.BasicLogRecord;

import java.util.Iterator;

import static com.wang.tx.recovery.LogRecord.*;

/**
 * 从后往前遍历日志
 */
public class LogRecordIterator implements Iterator<LogRecord> {
    // 先获得一个BasicLogRecord迭代器
    // 此迭代器迭代得到结果是一条条raw的日志记录
    private Iterator<BasicLogRecord> iter = MiniDB.getLm().iterator();


    @Override
    public boolean hasNext() {
        return iter.hasNext();
    }

    @Override
    public LogRecord next() {
        BasicLogRecord blr = iter.next();
        int op = blr.nextInt();
        switch (op) {
            case CHECKPOINT:
                return new CheckpointRecord(blr);
            case START:
                return new StartRecord(blr);
            case COMMIT:
                return new CommitRecord(blr);
            case ROLLBACK:
                return new RollBackRecord(blr);
            case SETINT:
                return new SetIntRecord(blr);
            case SETSTRING:
                return new SetStringRecord(blr);
            default:
                return null;
        }

    }

    public void remove() {
        throw new UnsupportedOperationException();
    }
}