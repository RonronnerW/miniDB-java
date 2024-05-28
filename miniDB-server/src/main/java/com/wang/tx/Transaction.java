package com.wang.tx;

import com.wang.MiniDB;
import com.wang.buffer.Buffer;
import com.wang.buffer.PageFormatter;
import com.wang.file.Block;
import com.wang.tx.concurrency.ConcurrencyManager;
import com.wang.tx.recovery.RecoveryManager;

import java.io.IOException;

public class Transaction {
    /**
     * 下一个事务id
     */
    private static int nextTxNum = 0;
    /**
     * 恢复管理器
     */
    private RecoveryManager recoveryMgr;
    /**
     * 并发管理器
     */
    private ConcurrencyManager concurMgr;
    /**
     * 事务id
     */
    private int txNum;
    /**
     * 事务相关的缓冲区
     */
    private BufferList myBuffers = new BufferList();

    public Transaction() {
        txNum = nextTxNumber();
        recoveryMgr = new RecoveryManager(txNum);
        concurMgr = new ConcurrencyManager();

    }

    public int getTxNum() {
        return txNum;
    }

    /**
     * 提交
     * 1. 提交
     * 2. 取消固定所有缓冲区
     * 3. 释放当前事务所有的锁
     */
    public void commit() {
        recoveryMgr.commit();
        myBuffers.unpinAll();
        concurMgr.release();
        System.out.println("transaction " + txNum + " committed");
    }

    /**
     * 回滚
     * @throws InterruptedException
     */
    public void rollback() throws InterruptedException {
        recoveryMgr.rollback();
        myBuffers.unpinAll();
        concurMgr.release();
        System.out.println("transaction " + txNum + " rolled back");
    }

    /**
     * 恢复
     * @throws InterruptedException
     */
    public void recover() throws InterruptedException {
        MiniDB.getBm().flushAll(txNum);
        recoveryMgr.recover();
    }

    /**
     * 固定缓冲区
     * @param blk
     * @throws InterruptedException
     */
    public void pin(Block blk) throws InterruptedException {
        myBuffers.pin(blk);
    }

    public void unpin(Block blk) {
        myBuffers.unpin(blk);
    }

    /**
     * 首先获取对应块的共享锁，然后读
     * @param blk
     * @param offset
     * @return
     */
    public int getInt(Block blk, int offset) {
        concurMgr.sLock(blk);
        Buffer buff = myBuffers.getBuffer(blk);
        return buff.getInt(offset);
    }

    public String getString(Block blk, int offset) {
        concurMgr.sLock(blk);
        Buffer buff = myBuffers.getBuffer(blk);
        return buff.getString(offset);
    }

    /**
     * 首先获取对应块的排他锁，然后追加日志，最后写
     * @param blk
     * @param offset
     * @param val
     */
    public void setInt(Block blk, int offset, int val) {
        concurMgr.xLock(blk);
        Buffer buff = myBuffers.getBuffer(blk);
        // 返回追加一条日志记录后的LSN
        int lsn = recoveryMgr.setInt(buff, offset, val);
        buff.setInt(offset, val, txNum, lsn);
    }

    public void setString(Block blk, int offset, String val) {
        concurMgr.xLock(blk);
        Buffer buff = myBuffers.getBuffer(blk);
        // 返回追加一条日志记录后的LSN
        int lsn = recoveryMgr.setString(buff, offset, val);
        buff.setString(offset, val, txNum, lsn);
    }

    /**
     * 获取文件的大小，即块的数量
     * 解决幻读现象：给一个结束标记符上锁，size()需要先获得共享锁才能调用
     * @param fileName 指定文件名
     * @return
     * @throws IOException
     */
    public int size(String fileName) throws IOException {
        // 模拟的文件EOF
        Block dummyBlk = new Block(fileName, -1);
        concurMgr.sLock(dummyBlk);
        return MiniDB.getFm().size(fileName);
    }

    /**
     * 解决幻读现象 在append调用之前需要先获得结束标记符的排他锁
     * @param fileName
     * @param pfmt
     * @return
     * @throws InterruptedException
     */
    public Block append(String fileName, PageFormatter pfmt) throws InterruptedException {
        // 模拟的文件EOF
        Block dummyBlk = new Block(fileName, -1);
        concurMgr.xLock(dummyBlk);

        Block blk = myBuffers.pinNew(fileName, pfmt);
        unpin(blk);
        return blk;
    }

    private static synchronized int nextTxNumber() {
        nextTxNum++;
        System.out.println("new transaction: " + nextTxNum);
        return nextTxNum;
    }

}