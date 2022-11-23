package it.unitn.disi.ds1.project.Messages;

import it.unitn.disi.ds1.project.Common;

import java.io.Serializable;

/**
 * Message used to tell a cache to crash
 */
public class CrashMsg implements Serializable {
    public final Common.CrashType nextCrash;
    public final Common.CrashTime nextCrashWhen;
    public CrashMsg(Common.CrashType nextCrash, Common.CrashTime nextCrashWhen) {
        this.nextCrash = nextCrash;
        this.nextCrashWhen = nextCrashWhen;
    }
}
