package com.demo.outbox.pipeline;

import com.demo.outbox.pipeline.context.CardApplicationContext;
import com.demo.outbox.pipeline.step.*;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;

import static org.assertj.core.api.Assertions.*;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@DisplayName("PipelineRegistry unit tests")
class PipelineRegistryTest {

    @SuppressWarnings("unchecked")
    private PipelineStep<CardApplicationContext> step(String pipeline, int index) {
        PipelineStep<CardApplicationContext> s = mock(PipelineStep.class);
        when(s.getPipelineType()).thenReturn(pipeline);
        // lenient: single-element lists are never compared, so getStepIndex() may not be called
        lenient().when(s.getStepIndex()).thenReturn(index);
        return s;
    }

    @Test
    @DisplayName("getSteps returns steps in step-index order")
    void getSteps_returnsSortedByIndex() {
        var s2 = step("PIPELINE_A", 2);
        var s0 = step("PIPELINE_A", 0);
        var s1 = step("PIPELINE_A", 1);

        PipelineRegistry registry = new PipelineRegistry(List.of(s2, s0, s1));

        List<PipelineStep<?>> steps = registry.getSteps("PIPELINE_A");
        assertThat(steps).extracting(PipelineStep::getStepIndex).containsExactly(0, 1, 2);
    }

    @Test
    @DisplayName("getSteps returns empty list for unknown pipeline")
    void getSteps_returnsEmptyForUnknown() {
        PipelineRegistry registry = new PipelineRegistry(List.of(step("PIPELINE_A", 0)));
        assertThat(registry.getSteps("UNKNOWN")).isEmpty();
    }

    @Test
    @DisplayName("hasPipeline returns true for registered pipeline")
    void hasPipeline_trueForRegistered() {
        PipelineRegistry registry = new PipelineRegistry(List.of(step("PIPELINE_A", 0)));
        assertThat(registry.hasPipeline("PIPELINE_A")).isTrue();
        assertThat(registry.hasPipeline("PIPELINE_B")).isFalse();
    }

    @Test
    @DisplayName("multiple pipelines are isolated from each other")
    void multiplePipelines_isolated() {
        var a0 = step("PIPELINE_A", 0);
        var a1 = step("PIPELINE_A", 1);
        var b0 = step("PIPELINE_B", 0);

        PipelineRegistry registry = new PipelineRegistry(List.of(a0, a1, b0));

        assertThat(registry.getSteps("PIPELINE_A")).hasSize(2);
        assertThat(registry.getSteps("PIPELINE_B")).hasSize(1);
    }

    @Test
    @DisplayName("logRegisteredPipelines emits WARN for single-step pipeline without throwing")
    void logRegisteredPipelines_singleStep_logsWarnWithoutException() {
        PipelineRegistry registry = new PipelineRegistry(List.of(step("SINGLE_STEP", 0)));
        assertThatCode(() -> registry.logRegisteredPipelines()).doesNotThrowAnyException();
    }

    @Test
    @DisplayName("logRegisteredPipelines emits INFO for multi-step pipeline without throwing")
    void logRegisteredPipelines_multiStep_logsInfoWithoutException() {
        PipelineRegistry registry = new PipelineRegistry(List.of(step("MULTI", 0), step("MULTI", 1)));
        assertThatCode(() -> registry.logRegisteredPipelines()).doesNotThrowAnyException();
    }
}
