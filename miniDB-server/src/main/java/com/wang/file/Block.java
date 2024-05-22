package com.wang.file;

import java.util.Objects;

/**
 * 文件块类
 * 通过给定文件名和逻辑块号唯一标识一个特定的文件块
 */
public class Block {
    /**
     * 文件名
     */
    private String fileName;

    /**
     * 逻辑块号
     */
    private int blockNum;

    public Block(String fileName, int blockNum) {
        this.fileName = fileName;
        this.blockNum = blockNum;
    }

    public String getFileName() {
        return fileName;
    }

    public int getBlockNum() {
        return blockNum;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null || getClass() != object.getClass()) return false;
        Block block = (Block) object;
        return blockNum == block.blockNum && Objects.equals(fileName, block.fileName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fileName, blockNum);
    }

    @Override
    public String toString() {
        return "Block{" +
                "fileName='" + fileName + '\'' +
                ", blockNum=" + blockNum +
                '}';
    }
}
