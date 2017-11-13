package com.insto.thriftpool;

import com.google.common.base.Strings;
import lombok.Getter;
import lombok.Setter;

public class PingInfo {

    @Getter @Setter private long pingStartDelayInSec = 10;
    @Getter @Setter private long pingIntervalInSec = 10;
    @Getter @Setter private String pingMethodName = "ping";

    private PingInfo() {

    }

    public boolean isValid() {
        if(Strings.isNullOrEmpty(pingMethodName)) {
            return false;
        }
        if(pingIntervalInSec <= 0) {
            return false;
        }
        if(pingStartDelayInSec < 0) {
            return false;
        }
        return true;
    }

    public static class PingInfoBuilder {

        private PingInfo pingInfo = new PingInfo();

        public PingInfoBuilder pingStartDelayInSec(long pingStartDelayInSec){
            this.pingInfo.setPingStartDelayInSec(pingStartDelayInSec);
            return this;
        }

        public PingInfoBuilder pingIntervalInSec(long pingIntervalInSec){
            this.pingInfo.setPingIntervalInSec(pingIntervalInSec);
            return this;
        }

        public PingInfoBuilder pingMethodName(String pingMethodName){
            this.pingInfo.setPingMethodName(pingMethodName);
            return this;
        }

        public PingInfo build() {
            return this.pingInfo;
        }
    }

}
