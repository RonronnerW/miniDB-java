package com.wang.file;

import com.wang.MiniDB;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

/**
 * 文件页类
 * 文件页是操作系统和文件系统在内存中管理文件数据的单位，通常用于虚拟内存和文件缓存。
 */
public class Page<T> {
    /**
     * 一个块的字节数
     * 在商用的数据库系统中，这个值通常被设置为和OS块同样的大小，一个典型的值就是4K字节。
     */
    public static final int BLOCK_SIZE = 400;

    /**
     * int类型数据长度 4
     */
    public static final int INT_SIZE = Integer.SIZE / Byte.SIZE;

    /**
     * string类型数据长度
     * 指示字符串长度的整数 + 各字符占的字节数
     * @param n
     * @return
     */
    public static final int STR_SIZE(int n) {
        float bytesPerChar = Charset.defaultCharset().newEncoder().maxBytesPerChar();
        // 指示字符串长度的整数 + 各字符占的字节数
        return INT_SIZE + n * ((int) bytesPerChar);
    }

    /**
     * 页中的内容
     * 一个ByteBuffer对象会跟踪缓冲区的当前位置指针，可以通过position()方法来改变它的位置
     */
    private ByteBuffer contents = ByteBuffer.allocateDirect(BLOCK_SIZE);

    /**
     * 文件管理器
     */
    private FileManager fileMgr = MiniDB.getFm();


    /**
     * 页粒度的并发锁
     * 假设有两个JDBC的客户端，每个客户端都运行着他们自己的线程，并且都尝试读不同的整数到一个相同的内存页中。
     * 线程A先执行，它开始执行getInt()方法，但是在运行完该方法中的第一行后被打断了，也就是说，position已经被设置好了，但是还没开始真正读数据；
     * 这个时候线程B紧接着执行，并且也执行了getInt()方法，直到这个方法执行完毕，这个时候的position会被指向线程B想指的地方；
     * 现在线程A继续执行，可是position位置已经被B改变了，线程A却全然不知，因此线程A继续读出来的数据就会是错的
     */
    public synchronized int getInt(int offset) {
        contents.position(offset);
        return contents.getInt();
    }

    public synchronized void setInt(int offset, int val) {
        contents.position(offset);
        contents.putInt(val);
    }

    public synchronized String getString(int offset) {
        contents.position(offset);
        int len = contents.getInt();  // 先获取字符串的长度
        byte[] byteVal = new byte[len];  // 再获取字符串中的后续字符
        contents.get(byteVal);
        return new String(byteVal);
    }

    public synchronized void setString(int offset, String val) {
        contents.position(offset);
        byte[] byteVal = val.getBytes();
        int len = byteVal.length;  // 获取字符串的长度

        contents.putInt(len);
        contents.put(byteVal);
    }

    public Page() {
    }

    public synchronized void read(Block blk) {
        fileMgr.read(blk, contents);
    }

    public synchronized void write(Block blk) {
        fileMgr.write(blk, contents);
    }

    public synchronized Block append(String fileName) {
        return fileMgr.append(fileName, contents);
    }
}
