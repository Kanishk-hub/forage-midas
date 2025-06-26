package com.jpmc.midascore;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.foundation.Incentive;
import com.jpmc.midascore.repository.UserRepository;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.util.Optional;

@Component
public class TransactionListener {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Autowired
    private UserRepository userRepository;

    @Autowired
    private RestTemplate restTemplate;

    @KafkaListener(topics = "${general.kafka-topic}", groupId = "midas")
    public void handleTransaction(String message) {
        try {
            Transaction tx = objectMapper.readValue(message, Transaction.class);
            System.out.println("📥 Received: " + tx);

            Optional<UserRecord> senderOpt = userRepository.findById(tx.getSenderId());
            Optional<UserRecord> recipientOpt = userRepository.findById(tx.getRecipientId());

            if (senderOpt.isEmpty() || recipientOpt.isEmpty()) return;

            UserRecord sender = senderOpt.get();
            UserRecord recipient = recipientOpt.get();

            if (sender.getBalance() < tx.getAmount()) return;

            // 🧾 Deduct transaction amount from sender
            sender.setBalance(sender.getBalance() - tx.getAmount());

            // 🎁 Get incentive from external API
            Incentive incentive = restTemplate.postForObject(
                "http://localhost:8080/incentive",
                tx,
                Incentive.class
            );

            float incentiveAmt = (incentive != null) ? incentive.getAmount() : 0f;

            // 💰 Add transaction amount + incentive to recipient
            recipient.setBalance(recipient.getBalance() + tx.getAmount() + incentiveAmt);

            // 💾 Save both users
            userRepository.save(sender);
            userRepository.save(recipient);

            // ✅ Print Wilbur's balance if he exists
            userRepository.findAll().forEach(user -> {
                if (user.getName().equalsIgnoreCase("wilbur")) {
                    System.out.println("🌟 WILBUR BALANCE: " + (int) user.getBalance());
                }
            });

        } catch (Exception e) {
            System.out.println(" Error parsing transaction: " + e.getMessage());
        }
    }
}
