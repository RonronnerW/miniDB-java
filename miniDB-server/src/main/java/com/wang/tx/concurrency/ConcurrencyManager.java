package com.wang.tx.concurrency;

import com.wang.file.Block;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * 并发管理器 - 块级
 */
public class ConcurrencyManager {
    /**
     * 全局锁表 所有事务共享
     */
    private static LockTable lockTable = new LockTable();

    /**
     * 当前事务持有锁的情况
     */
    Map<Block, String> locks = new HashMap<>();

    /**
     * 请求共享锁
     * 当前事务未持有锁才会申请共享锁
     *
     * @param blk
     */
    public void sLock(Block blk) {
        if(null == locks.get(blk)) {
            lockTable.sLock(blk);
            locks.put(blk, "S");
        }

    }

    /**
     * 请求排他锁
     * 当前事务没有排他锁才加排他锁
     * 先获得共享锁然后升级为排他锁 TODO 不是很理解
     * @param blk
     */
    public void xLock(Block blk) {
        if(!hasXLock(blk)) {
            sLock(blk);
            lockTable.xLock(blk);
            locks.put(blk, "X");
        }
    }

    /**
     * 释放当前事务持有的所有锁
     */
    public void release() {
        for (Block block : locks.keySet()) {
            lockTable.unLock(block);
        }
        locks.clear();
    }
    private boolean hasXLock(Block blk) {
        String lockType = locks.get(blk);
        return (lockType != null && lockType.equals("X"));
    }
}
