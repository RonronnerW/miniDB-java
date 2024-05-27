package com.wang.tx.recovery;

import com.wang.MiniDB;
import com.wang.log.BasicLogRecord;
import com.wang.log.LogManager;

public class StartRecord implements LogRecord{
    private int myTxNum;

    public StartRecord(int myTxNum) {
        this.myTxNum = myTxNum;
    }

    /**
     * 根据一条BasicLogRecord来构造一条StartRecord。
     * 该构造函数是为了给 恢复/回滚 算法调用
     * <p>
     * 注意，一条开始日志记录的格式为：
     * <p>
     * <START,txNum>
     *
     * @param blr
     */
    public StartRecord(BasicLogRecord blr) {
        myTxNum = blr.nextInt();
    }

    /**
     * 将一条日志记录写到日志文件，返回对应的LSN
     *
     * @return 日志记录的 LSN
     */
    @Override
    public int writeToLog() {
        Object[] rec = new Object[]{START, myTxNum};
        LogManager logMgr = MiniDB.getLm();
        return logMgr.append(rec);
    }

    @Override
    public int op() {
        return START;
    }

    @Override
    public int txNumber() {
        return myTxNum;
    }

    @Override
    public void undo() {
        // 空方法即可
    }

    public String toString() {
        return "<START " + myTxNum + ">";
    }
}
