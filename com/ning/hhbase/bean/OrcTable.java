package com.ning.hhbase.bean;

public class OrcTable extends Table {
    
    /**
     * 分区字段
     */
    private Column partitionColumn = null;

    /**
     * 分桶字段
     */
    private Column bucketColumn = null;
    
    public Column getPartitionColumn() {
        return partitionColumn;
    }

    public void setPartitionColumn(Column partitionColumn) {
        this.partitionColumn = partitionColumn;
    }

    public Column getBucketColumn() {
        return bucketColumn;
    }

    public void setBucketColumn(Column bucketColumn) {
        this.bucketColumn = bucketColumn;
    }
    
    

}
