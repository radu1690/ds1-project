package it.unitn.disi.ds1.project.Messages;

public class CheckConsistencyResponseMsg extends Message {
    public Integer value;
    public CheckConsistencyResponseMsg(Integer dataId, Integer value) {
        super(dataId, null);
        this.value = value;
    }
}
