package com.demo.outbox.repository;

import com.demo.outbox.entity.CardApplication;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.UUID;

@Repository
public interface CardApplicationRepository extends JpaRepository<CardApplication, UUID> {
}
