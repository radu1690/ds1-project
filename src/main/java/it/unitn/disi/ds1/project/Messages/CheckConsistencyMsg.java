package it.unitn.disi.ds1.project.Messages;

/**
 * Messaged used to check consistency
 */
public class CheckConsistencyMsg extends Message {
    public CheckConsistencyMsg(Integer dataId) {
        super(dataId, null);
    }
}
