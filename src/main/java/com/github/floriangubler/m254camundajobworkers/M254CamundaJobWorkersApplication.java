package com.github.floriangubler.m254camundajobworkers;

import io.camunda.zeebe.client.ZeebeClient;
import io.camunda.zeebe.client.ZeebeClientBuilder;
import io.camunda.zeebe.client.api.response.ActivatedJob;
import io.camunda.zeebe.client.api.worker.JobClient;
import io.camunda.zeebe.client.api.worker.JobHandler;
import io.camunda.zeebe.client.api.worker.JobWorker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Scanner;

public class M254CamundaJobWorkersApplication {

    private final static Logger Logger = LoggerFactory.getLogger(M254CamundaJobWorkersApplication.class);

    private final static String jobType = "messaging-job";

    public static void main(final String[] args) {

        final ZeebeClientBuilder clientBuilder = ZeebeClient.newClientBuilder().gatewayAddress(System.getenv("ZEEBE_ADDRESS"));

        try (final ZeebeClient zeebeClient = clientBuilder.build()) {

            Logger.info("Opening job worker.");

            try (final JobWorker workerRegistration =
                         zeebeClient
                                 .newWorker()
                                 .jobType(jobType)
                                 .handler(new ProcessMessagingJobHandler(zeebeClient))
                                 .timeout(Duration.ofSeconds(10))
                                 .open()) {

                Logger.info("Job worker opened and receiving jobs.");

                // run until System.in receives exit command
                waitUntilSystemInput("exit");
            } catch (Throwable t){
                t.printStackTrace();
            }
        } catch (Throwable t){
            t.printStackTrace();
        }
    }

    private static void waitUntilSystemInput(final String exitCode) {
        try (final Scanner scanner = new Scanner(System.in)) {
            while (scanner.hasNextLine()) {
                final String nextLine = scanner.nextLine();
                if (nextLine.contains(exitCode)) {
                    return;
                }
            }
        } catch (Throwable t){
            t.printStackTrace();
        }
    }

    private static class ProcessMessagingJobHandler implements JobHandler {

        final ZeebeClient zeebeClient;

        private ProcessMessagingJobHandler(ZeebeClient zeebeClient) {
            this.zeebeClient = zeebeClient;
        }

        @Override
        public void handle(final JobClient client, final ActivatedJob job) {
            Logger.info("New job - correlating message '" + job.getCustomHeaders().get("msg_name") + "'");
            //Correlate message
            zeebeClient
                    .newPublishMessageCommand()
                    .messageName(job.getCustomHeaders().get("msg_name"))
                    .correlationKey(job.getVariable("correlation").toString())
                    .variable("process", job.getVariable("process"))
                    .timeToLive(Duration.ZERO)
                    .send();
            //End job
            client.newCompleteCommand(job.getKey()).send().join();
        }
    }
}