package com.demo.outbox.pipeline;

import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Discovers all {@link PipelineStep} beans at startup and organises them
 * by pipeline type in step-index order.
 *
 * Adding a new pipeline = register new step beans. Zero changes here.
 */
@Service
@Slf4j
public class PipelineRegistry {

    private final Map<String, List<PipelineStep<?>>> pipelines;

    public PipelineRegistry(List<PipelineStep<?>> allSteps) {
        this.pipelines = allSteps.stream()
            .collect(Collectors.groupingBy(
                PipelineStep::getPipelineType,
                Collectors.collectingAndThen(
                    Collectors.toList(),
                    steps -> steps.stream()
                        .sorted(Comparator.comparingInt(PipelineStep::getStepIndex))
                        .collect(Collectors.toList())
                )
            ));
    }

    @PostConstruct
    public void logRegisteredPipelines() {
        pipelines.forEach((type, steps) -> {
            if (steps.size() == 1) {
                log.warn("Pipeline [{}] has only 1 step ({}) — checkpoint is redundant but functional",
                    type, steps.get(0).getClass().getSimpleName());
            } else {
                log.info("Pipeline registered: [{}] with {} steps: {}",
                    type,
                    steps.size(),
                    steps.stream()
                        .map(s -> s.getStepIndex() + ":" + s.getClass().getSimpleName())
                        .collect(Collectors.joining(", "))
                );
            }
        });
    }

    public List<PipelineStep<?>> getSteps(String pipelineType) {
        return pipelines.getOrDefault(pipelineType, List.of());
    }

    public boolean hasPipeline(String pipelineType) {
        return pipelines.containsKey(pipelineType);
    }
}
