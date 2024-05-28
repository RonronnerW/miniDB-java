package com.wang.tx.concurrency;

import com.wang.file.Block;

import java.util.HashMap;
import java.util.Map;

/**
 * 锁表-对块进行加解锁 其他事务不能对块操作
 */
public class LockTable {
    /**
     * 当前块持有的锁
     */
    Map<Block, Integer> locks = new HashMap<>();

    /**
     * 请求共享锁
     *
     * @param blk
     */
    public synchronized void sLock(Block blk) {

        try {
            long timeMillis = System.currentTimeMillis();
            // 如果当前块有排他锁，则进入等待队列等待锁
            while (hasXLock(blk) && !waitTooLong(timeMillis)) {
                wait(ConcurrencyConstant.TIME_OUT);
            }
            // 死锁或者其他原因导致 超时
            if(hasXLock(blk)) {
                throw new LockAbortException();
            }
            // 这个val肯定是个非负的值
            // 1. 如果这个块之前没有访问过，即lockVal=0
            // 2. 如果这个块的共享锁已经被持有，则lockVal > 0
            int val = getLockVal(blk);
            locks.put(blk, val + 1);
        } catch (InterruptedException e) {
            throw new LockAbortException(e);
        }

    }

    /**
     * 请求排他锁
     *
     * @param blk
     */
    public synchronized void xLock(Block blk) {
        try {
            long timeMillis = System.currentTimeMillis();
            // TODO 为什么不判断是否占有排他锁呢？？？
            // 如果当前块有共享锁，则进入等待队列等待锁
            while ( hasSLocks(blk) && !waitTooLong(timeMillis)) {
                wait(ConcurrencyConstant.TIME_OUT);
            }
            // 死锁或者其他原因导致 超时
            if( hasSLocks(blk)) {
                throw new LockAbortException();
            }

            locks.put(blk, -1); // 获得互斥锁，把锁表置为-1
        } catch (InterruptedException e) {
            throw new LockAbortException(e);
        }
    }

    /**
     * 释放锁
     */
    public synchronized void unLock(Block blk) {
        Integer num = locks.get(blk);
        if(num>1) {
            locks.put(blk, num-1);
        } else {
            // num=-1 释放排他锁
            // num=1 释放共享锁
            locks.remove(blk);
            notifyAll();
        }
    }

    /**
     * 判断指定块的互斥锁是否已经被占用。
     * getLockVal(block) 为 -1 时表示互斥锁被占用。
     *
     * @param block
     * @return
     */
    private boolean hasXLock(Block block) {
        return getLockVal(block) < 0;
    }


    /**
     * 判断指定块的共享锁是否已经被持有。
     * getLockVal(block) 为被持有的次数。
     *
     * @param block
     * @return
     */
    private boolean hasSLocks(Block block) {
        // TODO 为什么是 > 1， 而不是 > 0
        return getLockVal(block) > 1;
    }

    private int getLockVal(Block block) {
        Integer val = locks.get(block);
        if (null == val)
            return 0;
        return val;
    }

    /**
     * 超时判断
     *
     * @param startTime
     * @return
     */
    private boolean waitTooLong(long startTime) {
        return System.currentTimeMillis() - startTime > ConcurrencyConstant.TIME_OUT;
    }
}
