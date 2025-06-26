package com.jpmc.midascore.controller; // ✅ Correct package declaration

import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Balance;
import com.jpmc.midascore.repository.UserRepository;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.Optional;

@RestController
public class BalanceController {

    @Autowired
    private UserRepository userRepository;

    @GetMapping("/balance")
    public Balance getBalance(@RequestParam("userId") int userId) {
        Optional<UserRecord> userOpt = userRepository.findById((long) userId);


        float amount = userOpt.map(UserRecord::getBalance).orElse(0f);
        return new Balance(amount);
    }
}
