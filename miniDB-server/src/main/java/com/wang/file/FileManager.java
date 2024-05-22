package com.wang.file;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Map;

/**
 * 处理与OS的交互
 * 文件管理器总是以一个块大小的字节序列为单位，从相应的文件进行读/写的，而且总是在一个块的边界开始操作。
 * 这样做时，文件管理器将确保每次读取、写入或追加调用都只需要一次磁盘访问。
 * 整个系统中 总是只有一个FileMgr对象会被创建
 */
public class FileManager {
    /**
     * 数据库文件目录
     */
    private File dbDirectory;

    /**
     * 是否新创建的数据库
     */
    private boolean isNew;

    /**
     * 文件hash，处理过的文件放入hash表
     */
    private Map<String, FileChannel> openFiles = new HashMap<>();

    /**
     * 构造函数指明数据库文件放在哪
     * @param dbname
     */
    public FileManager(String dbname) {
        // 默认路径为当前项目目录下
        String homedir = System.getProperty("user.dir");
        dbDirectory = new File(homedir, dbname);
        isNew = !dbDirectory.exists();

        // 如果是新的数据库，则创建
        if (isNew && !dbDirectory.mkdir())
            throw new RuntimeException("Cannot create " + dbname);
        // 删除临时表文件
        for (String filename : dbDirectory.list()) {
            if (filename.startsWith("temp"))
                new File(dbDirectory, filename).delete();
        }

    }

    public FileManager() {
    }

    public boolean isNew() {
        return isNew;
    }

    /**
     * 返回的是指定文件的块数，它将允许客户端直接移动到文件的末尾。
     * @param fileName
     * @return
     * @throws IOException
     */
    public int size(String fileName) throws IOException {
        FileChannel fc = getFile(fileName);
        return (int) fc.size() / Page.BLOCK_SIZE;
    }

    private synchronized FileChannel getFile(String fileName) throws IOException {
        FileChannel fc = openFiles.get(fileName);
        // 如果map中没打开过
        if (fc == null) {
            File dbTable = new File(dbDirectory, fileName);
            // 一个文件在被使用之前必须被打开，SimpleDB的文件管理器会创建一个RandomAccessFile对象，然后再获得该文件的文件通道
            // 以“rws”的模式打开的，“rw”部分表示这个文件是为了读和写而打开，“s”表示告诉OS不要进行为了优化磁盘性能而作出的磁盘I/O延迟
            RandomAccessFile f = new RandomAccessFile(dbTable, "rws");
            fc = f.getChannel();
            openFiles.put(fileName, fc);
        }

        return fc;
    }

    synchronized void read(Block block, ByteBuffer buffer) {
        try {
            buffer.clear();
            FileChannel fc = getFile(block.getFileName());
            fc.read(buffer, block.getBlockNum() * buffer.capacity());
        } catch (IOException e) {
            throw new RuntimeException("Cannot read block " + block);
        }

    }

    synchronized void write(Block blk, ByteBuffer buffer) {
        try {
            buffer.rewind();
            FileChannel fc = getFile(blk.getFileName());
            fc.write(buffer, blk.getBlockNum() * buffer.capacity());
        } catch (IOException e) {
            throw new RuntimeException("Cannot write block " + blk);
        }
    }

    synchronized Block append(String filename, ByteBuffer buffer) {
        try {
            int newBlkNum = size(filename);
            Block newBlk = new Block(filename, newBlkNum);
            write(newBlk, buffer);
            return newBlk;
        } catch (IOException e) {
            throw new RuntimeException("Cannot append block to file " + filename);
        }
    }
}
