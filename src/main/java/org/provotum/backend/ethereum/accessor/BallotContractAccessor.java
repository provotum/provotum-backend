package org.provotum.backend.ethereum.accessor;

import org.provotum.backend.communication.message.base.Status;
import org.provotum.backend.communication.message.partial.Contract;
import org.provotum.backend.communication.socket.message.deployment.BallotDeploymentResponse;
import org.provotum.backend.communication.socket.message.event.ChangeEventResponse;
import org.provotum.backend.communication.socket.message.event.VoteEventResponse;
import org.provotum.backend.communication.socket.message.meta.GetQuestionResponse;
import org.provotum.backend.communication.socket.message.meta.GetResultResponse;
import org.provotum.backend.communication.socket.message.removal.BallotRemovalResponse;
import org.provotum.backend.communication.socket.message.state.CloseVoteEventResponse;
import org.provotum.backend.communication.socket.message.state.OpenVoteEventResponse;
import org.provotum.backend.communication.socket.message.vote.VoteResponse;
import org.provotum.backend.communication.socket.publisher.TopicPublisher;
import org.provotum.backend.config.EthereumConfiguration;
import org.provotum.backend.ethereum.config.BallotContractConfig;
import org.provotum.backend.ethereum.wrappers.Ballot;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.web3j.crypto.Credentials;
import org.web3j.protocol.Web3j;
import org.web3j.protocol.core.DefaultBlockParameterName;
import rx.Observer;
import rx.Scheduler;
import rx.schedulers.Schedulers;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

@Component
public class BallotContractAccessor extends AContractAccessor<Ballot, BallotContractConfig> {

    private static final Logger logger = Logger.getLogger(BallotContractAccessor.class.getName());

    private Web3j web3j;
    private TopicPublisher topicPublisher;
    private EthereumConfiguration ethereumConfiguration;

    private Scheduler subscriptionScheduler;
    private ExecutorService executorService;

    @Autowired
    public BallotContractAccessor(Web3j web3j, EthereumConfiguration ethereumConfiguration, TopicPublisher topicPublisher) {
        this.web3j = web3j;
        this.ethereumConfiguration = ethereumConfiguration;
        this.topicPublisher = topicPublisher;

        ExecutorService executor = Executors.newCachedThreadPool();
        this.subscriptionScheduler = Schedulers.from(executor);

        // executor for async tasks
        this.executorService = Executors.newCachedThreadPool();
    }

    @Override
    public void deploy(BallotContractConfig config) {
        // asynchronously deploy the contract and notify any clients
        // over a websocket connection
        logger.info("Starting ballot contract deployment in a new thread.");

        this.executorService.submit(() -> {
            logger.info("Ballot deployment in new thread started.");

            BallotDeploymentResponse response;

            Random rnd = new Random();
            boolean isSuccess = rnd.nextBoolean();

            if (isSuccess) {
                logger.info("Ballot deployment was successful.");
                response = new BallotDeploymentResponse(Status.SUCCESS, "Deployment successful", new Contract("ballot", "0x" + UUID.randomUUID()));

            } else {
                logger.severe("Failed to deploy ballot: " + "Test failure");
                response = new BallotDeploymentResponse(Status.ERROR, "Deployment failed: " + "Test failure", new Contract("ballot", null));
            }

            logger.info("Sending ballot deployment response to subscribers at topic " + TopicPublisher.DEPLOYMENT_TOPIC);
            // notify any subscribers about the new ballot
            this.topicPublisher.send(
                TopicPublisher.DEPLOYMENT_TOPIC,
                response
            );
        });
    }

    @Override
    public Ballot load(String contractAddress) {
        return Ballot.load(
            contractAddress,
            this.web3j,
            this.ethereumConfiguration.getWalletCredentials(),
            Ballot.GAS_PRICE,
            Ballot.GAS_LIMIT
        );
    }

    @Override
    public void remove(String contractAddress) {
        logger.info("Starting ballot contract removal in new thread.");

        // starting execution in a new thread to avoid blocking.
        this.executorService.submit(() -> {
            logger.info("Ballot contract removal in new thread started.");

            BallotRemovalResponse response;

            Random rnd = new Random();
            boolean isSuccess = rnd.nextBoolean();
            String trx = "0x" + UUID.randomUUID();

            if (isSuccess) {
                logger.info("Ballot contract removed. Transaction hash is: " + trx);
                response = new BallotRemovalResponse(Status.SUCCESS, "Successfully removed ballot.", trx);

            } else {
                response = new BallotRemovalResponse(Status.ERROR, "Failed to remove ballot at " + contractAddress + ": " + "Test failure", null);
            }

            logger.info("Sending ballot removal response to subscribers at topic " + TopicPublisher.REMOVAL_TOPIC);
            this.topicPublisher.send(
                TopicPublisher.REMOVAL_TOPIC,
                response
            );
        });
    }

    /**
     * Open the voting on a Ballot contract at the specified address.
     *
     * @param contractAddress The ballot's contract address.
     */
    public void openVoting(String contractAddress) {
        logger.info("Starting opening vote in new thread.");

        // starting execution in a new thread to avoid blocking.
        this.executorService.submit(() -> {
            logger.info("Opening vote in new thread started.");
            OpenVoteEventResponse response;

            Random rnd = new Random();
            boolean isSuccess = rnd.nextBoolean();
            String trx = "0x" + UUID.randomUUID();

            if (isSuccess) {
                logger.info("Vote opened. Transaction hash is " + trx);
                response = new OpenVoteEventResponse(Status.SUCCESS, "Opening vote was successful.", trx);
            } else {
                response = new OpenVoteEventResponse(Status.ERROR, "Opening vote failed: " + "Test failure", null);
            }

            logger.info("Sending open vote response to subscribers at topic " + TopicPublisher.EVENT_TOPIC);
            this.topicPublisher.send(
                TopicPublisher.EVENT_TOPIC,
                response
            );
        });
    }

    /**
     * Close the voting on a Ballot contract specified by its address.
     *
     * @param contractAddress The ballot's contract address.
     */
    public void closeVoting(String contractAddress) {
        logger.info("Starting closing vote in new thread.");

        // starting execution in a new thread to avoid blocking.
        this.executorService.submit(() -> {
            logger.info("Closing vote in new thread started.");
            CloseVoteEventResponse response;

            Random rnd = new Random();
            boolean isSuccess = rnd.nextBoolean();
            String trx = "0x" + UUID.randomUUID();

            if (isSuccess) {
                logger.info("Vote closed. Transaction hash is " + trx);

                response = new CloseVoteEventResponse(Status.SUCCESS, "Closing vote was successful.", trx);
            } else {
                response = new CloseVoteEventResponse(Status.ERROR, "Closing vote failed: " + "Test failure", null);
            }

            logger.info("Sending close vote response to subscribers at topic " + TopicPublisher.EVENT_TOPIC);
            this.topicPublisher.send(
                TopicPublisher.EVENT_TOPIC,
                response
            );
        });
    }

    /**
     * Vote on the ballot.
     *
     * @param contractAddress The address of the ballot.
     * @param vote            The vote.
     */
    public void vote(String contractAddress, BigInteger vote, Credentials credentials) {
        logger.info("Starting submitting vote in new thread.");

        // starting execution in a new thread to avoid blocking.
        this.executorService.submit(() -> {
            logger.info("Submitting vote in new thread started.");
            VoteResponse response;

            Random rnd = new Random();
            boolean isSuccess = rnd.nextBoolean();
            String trx = "0x" + UUID.randomUUID();

            if (isSuccess) {
                logger.info("Submitted vote. Transaction hash is " + trx);

                response = new VoteResponse(Status.SUCCESS, "Successfully submitted vote.", trx);
            } else {
                response = new VoteResponse(Status.ERROR, "Submitting vote failed: " + "Test failure", null);
            }

            logger.info("Sending vote response to subscribers at topic " + TopicPublisher.EVENT_TOPIC);
            this.topicPublisher.send(
                TopicPublisher.VOTE_TOPIC,
                response
            );
        });
    }

    /**
     * Get the voting results from the Ballot at the specified contract.
     *
     * @param contractAddress The address of the contract.
     */
    public void getResults(String contractAddress) {
        logger.info("Starting retrieving votes in new thread.");

        // starting execution in a new thread to avoid blocking.
        this.executorService.submit(() -> {
            logger.info("Retrieving votes in new thread started.");
            GetResultResponse response;

            Random rnd = new Random();
            boolean isSuccess = rnd.nextBoolean();
            String trx = "0x" + UUID.randomUUID();

            if (isSuccess) {
                logger.info("Submitted vote. Transaction hash is " + trx);

                Map<String, BigInteger> votes = new HashMap<>();
                votes.put("0x" + UUID.randomUUID(), BigInteger.ONE);
                votes.put("0x" + UUID.randomUUID(), BigInteger.ZERO);
                votes.put("0x" + UUID.randomUUID(), BigInteger.ZERO);

                response = new GetResultResponse(Status.SUCCESS, "Successfully fetched votes.", votes);
            } else {
                response = new GetResultResponse(Status.ERROR, "Fetching votes failed: " + "Test Failure", null);
            }

            logger.info("Sending get votes response to subscribers at topic " + TopicPublisher.META_TOPIC);
            this.topicPublisher.send(
                TopicPublisher.META_TOPIC,
                response
            );
        });
    }

    /**
     * Fetch the voting question from the Ballot contract.
     *
     * @param contractAddress The address of the ballot contract.
     */
    public void getQuestion(String contractAddress) {
        logger.info("Starting retrieving vote question in new thread.");

        // starting execution in a new thread to avoid blocking.
        this.executorService.submit(() -> {
            logger.info("Retrieving vote question in new thread started.");
            GetQuestionResponse response;

            Random rnd = new Random();
            boolean isSuccess = rnd.nextBoolean();
            String question = "Is Vitalik a genius?";

            if (isSuccess) {
                logger.info("Retrieved question: " + question);

                response = new GetQuestionResponse(Status.SUCCESS, "Successfully retrieved voting question.", question);
            } else {
                response = new GetQuestionResponse(Status.ERROR, "Retrieving vote question failed: " + "Test failure", null);
            }

            logger.info("Sending voting question response to subscribers at topic " + TopicPublisher.META_TOPIC);
            this.topicPublisher.send(
                TopicPublisher.META_TOPIC,
                response
            );
        });
    }

    private void subscribeToVoteEvent(Ballot ballot) {
        ballot.voteEventEventObservable(DefaultBlockParameterName.EARLIEST, DefaultBlockParameterName.LATEST)
            .subscribeOn(this.subscriptionScheduler)
            .subscribe(new Observer<Ballot.VoteEventEventResponse>() {
                @Override
                public void onCompleted() {
                    logger.info("Stopped receiving vote events for ballot at " + ballot.getContractAddress());
                }

                @Override
                public void onError(Throwable throwable) {
                    logger.severe("Got an error while observing vote events for ballot at " + ballot.getContractAddress() + ": " + throwable.getMessage());
                    throwable.printStackTrace();
                }

                @Override
                public void onNext(Ballot.VoteEventEventResponse voteEventEventResponse) {
                    logger.info("Sending vote event response to topic " + TopicPublisher.EVENT_TOPIC);

                    Status status = voteEventEventResponse.wasSuccessful ? Status.SUCCESS : Status.ERROR;

                    topicPublisher.send(
                        TopicPublisher.EVENT_TOPIC,
                        new VoteEventResponse(status, voteEventEventResponse.reason, voteEventEventResponse._from)
                    );
                }
            });
    }

    private void subscribeToChangeEvent(Ballot ballot) {
        ballot.changeEventEventObservable(DefaultBlockParameterName.EARLIEST, DefaultBlockParameterName.LATEST)
            .subscribeOn(this.subscriptionScheduler)
            .subscribe(new Observer<Ballot.ChangeEventEventResponse>() {
                @Override
                public void onCompleted() {
                    logger.info("Stopped receiving change events for ballot at " + ballot.getContractAddress());
                }

                @Override
                public void onError(Throwable throwable) {
                    logger.severe("Got an error while observing change events for ballot at " + ballot.getContractAddress() + ": " + throwable.getMessage());
                    throwable.printStackTrace();
                }

                @Override
                public void onNext(Ballot.ChangeEventEventResponse changeEventEventResponse) {
                    logger.info("Sending change event response to topic " + TopicPublisher.EVENT_TOPIC);

                    Status status = changeEventEventResponse.wasSuccessful ? Status.SUCCESS : Status.ERROR;

                    topicPublisher.send(
                        TopicPublisher.EVENT_TOPIC,
                        new ChangeEventResponse(status, changeEventEventResponse.reason, changeEventEventResponse._from)
                    );
                }
            });
    }
}
