package com.wang.tx.recovery;

import com.wang.MiniDB;
import com.wang.log.BasicLogRecord;
import com.wang.log.LogManager;

public class RollBackRecord implements LogRecord{
    private int myTxNum;

    public RollBackRecord(int myTxNum) {
        this.myTxNum = myTxNum;
    }

    /**
     * 根据一条BasicLogRecord来构造一条RollBackRecord。
     * 该构造函数是为了给 恢复/回滚 算法调用
     * <p>
     * 注意，一条提交日志记录的格式为：
     * <p>
     * <ROLLBACK,txNum>
     *
     * @param blr
     */
    public RollBackRecord(BasicLogRecord blr) {
        myTxNum = blr.nextInt();
    }

    @Override
    public int writeToLog() {
        Object[] rec = new Object[]{ROLLBACK, myTxNum};
        LogManager logMgr = MiniDB.getLm();
        return logMgr.append(rec);
    }

    @Override
    public int op() {
        return ROLLBACK;
    }

    @Override
    public int txNumber() {
        return myTxNum;
    }

    @Override
    public void undo() {
        // empty is OKay
    }

    public String toString() {
        return "<ROLLBACK " + myTxNum + ">";
    }
}
