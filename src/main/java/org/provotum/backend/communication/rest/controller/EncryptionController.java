package org.provotum.backend.communication.rest.controller;

import org.provotum.backend.communication.rest.message.vote.EncryptionRequest;
import org.provotum.backend.communication.rest.message.vote.EncryptionResponse;
import org.provotum.backend.security.CipherTextWrapper;
import org.provotum.backend.security.EncryptionManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

import javax.crypto.BadPaddingException;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.logging.Logger;

@RestController
public class EncryptionController {

    private static final Logger logger = Logger.getLogger(EncryptionController.class.getName());
    private static final String CONTEXT = "/encryption";

    private EncryptionManager encryptionManager;

    @Autowired
    public EncryptionController(EncryptionManager encryptionManager) {
        this.encryptionManager = encryptionManager;
    }

    @RequestMapping(value = CONTEXT + "/generate", method = RequestMethod.POST)
    @ResponseStatus(HttpStatus.CREATED)
    public EncryptionResponse encryptAndGenerateProof(@RequestBody EncryptionRequest encryptionRequest) {
        logger.info("Received request to encrypt and generate vote.");

        CipherTextWrapper cipherTextWrapper = null;
        try {
            cipherTextWrapper = this.encryptionManager.encryptVoteAndGenerateProof(encryptionRequest.getVote());
        } catch (NoSuchPaddingException | NoSuchAlgorithmException | InvalidKeyException | BadPaddingException | IllegalBlockSizeException e) {
            logger.severe("Failed to generate proof: " + e.getMessage());
            e.printStackTrace();

            return null;
        }

        return new EncryptionResponse(cipherTextWrapper.getCiphertext(), cipherTextWrapper.getProof(), cipherTextWrapper.getRandom());
    }
}
