package com.wang.tx.recovery;

import com.wang.MiniDB;
import com.wang.buffer.Buffer;
import com.wang.buffer.BufferManager;
import com.wang.file.Block;
import com.wang.log.BasicLogRecord;
import com.wang.log.LogManager;

public class SetIntRecord implements LogRecord {
    private int myTxNum;
    private int offset;

    private int val;
    private Block blk;

    public SetIntRecord(int myTxNum, Block blk, int offset, int val) {
        this.myTxNum = myTxNum;
        this.offset = offset;
        this.blk = blk;
        this.val = val;
    }

    /**
     * 根据一条BasicLogRecord来构造一条SetStringRecord。
     * 该构造函数是为了给 恢复/回滚 算法调用
     * 注意，一条更新日志记录的格式为：
     * <SETxxx,txNum,fileName,blkNum,offset,old value,new value>
     *
     * @param blr
     */
    public SetIntRecord(BasicLogRecord blr) {
        myTxNum = blr.nextInt();
        String fileName = blr.nextString();
        int blkNum = blr.nextInt();
        blk = new Block(fileName, blkNum);
        offset = blr.nextInt();
        val = blr.nextInt();
    }

    /**
     * 将一条日志记录写入日志文件，返回LSN
     *
     * @return
     */
    @Override
    public int writeToLog() {
        Object[] rec = new Object[]{SETINT, myTxNum,
                blk.getFileName(), offset, val};

        LogManager logMgr = MiniDB.getLm();
        return logMgr.append(rec);
    }

    /**
     * 返回日志记录的操作符。
     * <p>
     * CHECKPOINT = 0, START = 1,
     * COMMIT = 2, ROLLBACK = 3,
     * SETINT = 4, SETSTRING = 5;
     *
     * @return integer
     */
    @Override
    public int op() {
        return SETINT;
    }

    @Override
    public int txNumber() {
        return myTxNum;
    }

    @Override
    public void undo() throws InterruptedException {
        BufferManager bufferMgr = MiniDB.getBm();
        Buffer buff = bufferMgr.pin(blk);
        buff.setInt(offset, val, myTxNum, -1);
        bufferMgr.unpin(buff);
    }

    public String toString() {
        return "<SETINT " + myTxNum + " " + blk + " " + offset
               + " " + val + ">";
    }
    
}