
package cache.ivr.flink.poc.model.tiorder;

import org.apache.commons.lang3.builder.ToStringBuilder;

public class Metadata {

    public Integer RbaSqn;
    public Integer AuditSessionId;
    public String TableSpace;
    public Integer CURRENTSCN;
    public Integer SQLRedoLength;
    public Integer BytesProcessed;
    public String ParentTxnID;
    public String SessionInfo;
    public String RecordSetID;
    public String DBCommitTimestamp;
    public Integer COMMITSCN;
    public Integer SEQUENCE;
    public String Rollback;
    public String STARTSCN;
    public String SegmentName;
    public String OperationName;
    public Long TimeStamp;
    public String TxnUserID;
    public String RbaBlk;
    public String SegmentType;
    public String TableName;
    public String TxnID;
    public String Serial;
    public String ThreadID;
    public String COMMIT_TIMESTAMP;
    public String OperationType;
    public String ROWID;
    public String DBTimeStamp;
    public String TransactionName;
    public Double SCN;
    public Integer Session;

    @Override
    public String toString() {
        return new ToStringBuilder(this).append("RbaSqn", RbaSqn).append("AuditSessionId", AuditSessionId).append("TableSpace", TableSpace).append("CURRENTSCN", CURRENTSCN).append("SQLRedoLength", SQLRedoLength).append("BytesProcessed", BytesProcessed).append("parentTxnID", ParentTxnID).append("sessionInfo", SessionInfo).append("RecordSetID", RecordSetID).append("DBCommitTimestamp", DBCommitTimestamp).append("COMMITSCN", COMMITSCN).append("SEQUENCE", SEQUENCE).append("Rollback", Rollback).append("STARTSCN", STARTSCN).append("segmentName", SegmentName).append("operationName", OperationName).append("timeStamp", TimeStamp).append("txnUserID", TxnUserID).append("RbaBlk", RbaBlk).append("SegmentType", SegmentType).append("tableName", TableName).append("txnID", TxnID).append("serial", Serial).append("ThreadID", ThreadID).append("cOMMITTIMESTAMP", COMMIT_TIMESTAMP).append("operationType", OperationType).append("ROWID", ROWID).append("DBTimeStamp", DBTimeStamp).append("TransactionName", TransactionName).append("SCN", SCN).append("session", Session).toString();
    }

}
