package kinesis.v1.consumer;

import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;

class KinesisStreamConsumerWorkerWithId {

    private Worker worker;
    private String workerId;

    KinesisStreamConsumerWorkerWithId(Worker worker, String workerId) {
        this.worker = worker;
        this.workerId = workerId;
    }

    public Worker getWorker() {
        return worker;
    }

    public String getWorkerId() {
        return workerId;
    }
}
