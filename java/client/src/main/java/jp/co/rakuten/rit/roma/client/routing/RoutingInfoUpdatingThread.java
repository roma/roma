package jp.co.rakuten.rit.roma.client.routing;

public class RoutingInfoUpdatingThread extends Thread {

    private RoutingTable routingTable;
    public boolean stopped = false;
    private int pollingPeriod;

    public RoutingInfoUpdatingThread(RoutingTable routingTable) {
        this.routingTable = routingTable;
        pollingPeriod = 3000;
    }

    public void doStart() {
        start();
    }

    public void doStop() {
        stopped = true;
    }

    @Override
    public void run() {
        while (routingTable.enableLoop()) {
            if (stopped) {
                break;
            }
            try {
                Thread.sleep(pollingPeriod);
                routingTable.update();
            } catch (InterruptedException e) {
            } // ignore
        }
    }
}
