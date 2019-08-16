package k0.util.aws_kinesis;

import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;

class KinesisConsumerWorkerWithId {

    private Worker worker;
    private String workerId;

    KinesisConsumerWorkerWithId(Worker worker, String workerId) {
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
