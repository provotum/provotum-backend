package org.provotum.backend.ethereum.accessor;

import org.provotum.backend.communication.message.base.Status;
import org.provotum.backend.communication.socket.message.deployment.BallotDeploymentResponse;
import org.provotum.backend.communication.socket.message.event.ChangeEventResponse;
import org.provotum.backend.communication.socket.message.event.VoteEventResponse;
import org.provotum.backend.communication.socket.message.meta.GetQuestionResponse;
import org.provotum.backend.communication.socket.message.meta.GetResultResponse;
import org.provotum.backend.communication.message.partial.Contract;
import org.provotum.backend.communication.socket.message.removal.BallotRemovalResponse;
import org.provotum.backend.communication.socket.message.event.CloseVoteEventResponse;
import org.provotum.backend.communication.socket.message.event.OpenVoteEventResponse;
import org.provotum.backend.communication.socket.publisher.TopicPublisher;
import org.provotum.backend.config.EthereumConfiguration;
import org.provotum.backend.ethereum.config.BallotContractConfig;
import org.provotum.backend.ethereum.wrappers.Ballot;
import org.provotum.backend.communication.socket.message.vote.VoteResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.web3j.crypto.Credentials;
import org.web3j.protocol.Web3j;
import org.web3j.protocol.core.DefaultBlockParameterName;
import org.web3j.tuples.generated.Tuple2;
import rx.Observer;
import rx.Scheduler;
import rx.schedulers.Schedulers;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;
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

            Ballot ballot;
            BallotDeploymentResponse response;

            try {
                ballot = Ballot.deploy(
                    this.web3j,
                    this.ethereumConfiguration.getWalletCredentials(),
                    Ballot.GAS_PRICE,
                    Ballot.GAS_LIMIT,
                    config.getVotingQuestion(),
                    config.getZeroKnowledgeContractAddress()
                ).send();

                // TODO: we might have to check that we do not get events duplicated times if we deploy multiple ballots
                subscribeToVoteEvent(ballot);
                subscribeToChangeEvent(ballot);

                logger.info("Ballot deployment was successful. Contract address is: " + ballot.getContractAddress());
                response = new BallotDeploymentResponse(Status.SUCCESS, "Deployment successful", new Contract("ballot", ballot.getContractAddress()));
            } catch (Exception e) {
                logger.severe("Failed to deploy ballot: " + e.getMessage());
                e.printStackTrace();

                response = new BallotDeploymentResponse(Status.ERROR, "Deployment failed: " + e.getMessage(), new Contract("ballot", null));
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

            try {
                String trx = Ballot.load(
                    contractAddress,
                    this.web3j,
                    this.ethereumConfiguration.getWalletCredentials(),
                    Ballot.GAS_PRICE,
                    Ballot.GAS_LIMIT
                ).destroy().send().getTransactionHash();

                logger.info("Ballot contract removed. Transaction hash is: " + trx);
                response = new BallotRemovalResponse(Status.SUCCESS, "Successfully removed ballot.", trx);
            } catch (Exception e) {
                logger.severe("Failed to remove ballot contract: " + e.getMessage());
                e.printStackTrace();

                response = new BallotRemovalResponse(Status.ERROR, "Failed to remove ballot at " + contractAddress + ": " + e.getMessage(), null);
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

            try {
                String trx = Ballot.load(
                    contractAddress,
                    this.web3j,
                    this.ethereumConfiguration.getWalletCredentials(),
                    Ballot.GAS_PRICE,
                    Ballot.GAS_LIMIT
                ).openVoting().send().getTransactionHash();

                logger.info("Vote opened. Transaction hash is " + trx);

                response = new OpenVoteEventResponse(Status.SUCCESS, "Opening vote was successful.", trx);
            } catch (Exception e) {
                logger.severe("Failed to open vote on ballot contract at " + contractAddress);
                e.printStackTrace();

                response = new OpenVoteEventResponse(Status.ERROR, "Opening vote failed: " + e.getMessage(), null);
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

            try {
                String trx = Ballot.load(
                    contractAddress,
                    this.web3j,
                    this.ethereumConfiguration.getWalletCredentials(),
                    Ballot.GAS_PRICE,
                    Ballot.GAS_LIMIT
                ).closeVoting().send().getTransactionHash();

                logger.info("Vote closed. Transaction hash is " + trx);

                response = new CloseVoteEventResponse(Status.SUCCESS, "Closing vote was successful.", trx);
            } catch (Exception e) {
                logger.severe("Failed to close vote on ballot contract at " + contractAddress);
                e.printStackTrace();

                response = new CloseVoteEventResponse(Status.ERROR, "Closing vote failed: " + e.getMessage(), null);
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

            try {
                // TODO: we might need to subscribe again to vote events in the case when the ballot contract is not deployed but only referenced.
                String trx = Ballot.load(
                    contractAddress,
                    this.web3j,
                    credentials,
                    Ballot.GAS_PRICE,
                    Ballot.GAS_LIMIT
                ).vote(vote).send().getTransactionHash();

                logger.info("Submitted vote. Transaction hash is " + trx);

                response = new VoteResponse(Status.SUCCESS, "Successfully submitted vote.", trx);
            } catch (Exception e) {
                logger.severe("Failed to submit vote on ballot contract at " + contractAddress);
                e.printStackTrace();

                response = new VoteResponse(Status.ERROR, "Submitting vote failed: " + e.getMessage(), null);
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

            try {
                // TODO: we might need to subscribe again to vote events in the case when the ballot contract is not deployed but only referenced.
                Ballot ballot = Ballot.load(
                    contractAddress,
                    this.web3j,
                    this.ethereumConfiguration.getWalletCredentials(),
                    Ballot.GAS_PRICE,
                    Ballot.GAS_LIMIT
                );

                BigInteger totalVotes = ballot.getTotalVotes().send();
                logger.info("Fetched a total of " + totalVotes + " votes from the Ballot contract at " + contractAddress);

                Map<String, BigInteger> votes = new HashMap<>();
                for (BigInteger i = BigInteger.ZERO; i.compareTo(totalVotes) < 0; i = i.add(BigInteger.ONE)) {
                    logger.fine("Fetching vote at index " + i);
                    Tuple2<String, BigInteger> tuple = ballot.getVote(i).send();
                    logger.fine("Vote at index " + i + " fetched");

                    votes.put(tuple.getValue1(), tuple.getValue2());
                }

                response = new GetResultResponse(Status.SUCCESS, "Successfully fetched votes.", votes);
            } catch (Exception e) {
                logger.severe("Failed to submit vote on ballot contract at " + contractAddress);
                e.printStackTrace();

                response = new GetResultResponse(Status.ERROR, "Fetching votes failed: " + e.getMessage(), null);
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

            try {
                String question = Ballot.load(
                    contractAddress,
                    this.web3j,
                    this.ethereumConfiguration.getWalletCredentials(),
                    Ballot.GAS_PRICE,
                    Ballot.GAS_LIMIT
                ).getProposedQuestion().send();

                logger.info("Retrieved question: " + question);

                response = new GetQuestionResponse(Status.SUCCESS, "Successfully retrieved voting question.", question);
            } catch (Exception e) {
                logger.severe("Failed to retrieve voting question on ballot contract at " + contractAddress);
                e.printStackTrace();

                response = new GetQuestionResponse(Status.ERROR, "Retrieving vote question failed: " + e.getMessage(), null);
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
                    logger.info("Sending vote event response to topic '/topic/ballot/vote-event");

                    Status status = voteEventEventResponse.wasSuccessful ? Status.SUCCESS : Status.ERROR;

                    topicPublisher.send(
                        "/topic/ballot/vote-event",
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
                    logger.info("Sending change event response to topic '/topic/ballot/change-event");

                    Status status = changeEventEventResponse.wasSuccessful ? Status.SUCCESS : Status.ERROR;

                    topicPublisher.send(
                        "/topic/ballot/change-event",
                        new ChangeEventResponse(status, changeEventEventResponse.reason, changeEventEventResponse._from)
                    );
                }
            });
    }
}
