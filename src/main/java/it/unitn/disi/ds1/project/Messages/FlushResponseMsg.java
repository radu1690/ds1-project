package it.unitn.disi.ds1.project.Messages;

public class FlushResponseMsg extends Message {
    public FlushResponseMsg(Integer dataId, String requestId) {
        super(dataId, requestId);
    }

    @Override
    public String toString() {
        return "FlushResponseMsg{" +
                "dataId=" + dataId +
                ", requestId='" + requestId + '\'' +
                '}';
    }
}
