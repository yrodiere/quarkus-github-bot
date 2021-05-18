package io.quarkus.bot;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import javax.inject.Inject;

import org.jboss.logging.Logger;
import org.kohsuke.github.GHEvent;
import org.kohsuke.github.GHEventPayload;
import org.kohsuke.github.GHObject;
import org.kohsuke.github.GHWorkflowRun;
import org.kohsuke.github.GHWorkflowRun.Status;

import io.quarkiverse.githubapp.event.WorkflowRun;
import io.quarkus.bot.config.QuarkusBotConfig;

public class CancelDuplicateWorkflowRuns {

    private static final Logger LOG = Logger.getLogger(CancelDuplicateWorkflowRuns.class);

    @Inject
    QuarkusBotConfig quarkusBotConfig;

    void cancelDuplicateWorkflowRuns(@WorkflowRun.Requested GHEventPayload.WorkflowRun workflowRunPayload) {
        GHWorkflowRun workflowRun = workflowRunPayload.getWorkflowRun();

        List<GHWorkflowRun> workflowRunsToCancel;
        if (GHEvent.PUSH.equals(workflowRun.getEvent())) {
            // Push to a branch: we are mostly interested in periodically checking the code;
            // events are just a way to implement that without wasting resources.
            Optional<GHWorkflowRun> previousIncompleteRun = getPreviousIncompleteRuns(workflowRun).findAny();
            if (previousIncompleteRun.isPresent()) {
                // Cancel this new workflow run, because there is already one queued or in progress.
                // When the workflow run completes, we will restart the last cancelled one (if any).
                workflowRunsToCancel = Collections.singletonList(workflowRun);
                LOG.tracef(
                        "Workflow run #%d - Will cancel this new workflow run for branch '%s'"
                                + " because of existing workflow run #%s",
                        (Object) workflowRun.getId(), workflowRun.getHeadBranch(), previousIncompleteRun.get().getId());
            } else {
                // Let the new workflow run proceed,
                // because there aren't any similar runs that are queued or in progress.
                LOG.tracef(
                        "Workflow run #%d - Will let this new workflow run for branch '%s'"
                                + " because there are no existing workflow runs",
                        (Object) workflowRun.getId(), workflowRun.getHeadBranch());
                return;
            }
        } else if (GHEvent.PULL_REQUEST.equals(workflowRun.getEvent())) {
            // Pull request: we are only interested in checking the latest version of the code.
            // Cancel previous workflow runs whenever a new one appears.
            workflowRunsToCancel = getPreviousIncompleteRuns(workflowRun)
                    .collect(Collectors.toList());
            if (LOG.isTraceEnabled()) {
                LOG.tracef("Workflow run #%d - Will cancel workflow runs %s for branch '%s'"
                        + " because of this new workflow run",
                        workflowRun.getId(),
                        workflowRunsToCancel.stream().map(wr -> "#" + wr.getId()).collect(Collectors.joining(", ")),
                        workflowRun.getHeadBranch());
            }
        } else {
            return;
        }

        for (GHWorkflowRun workflowRunToCancel : workflowRunsToCancel) {
            try {
                if (!quarkusBotConfig.isDryRun()) {
                    LOG.tracef("Workflow run #%d - Cancelling workflow run #%d for branch %s",
                            workflowRun.getId(), workflowRunToCancel.getId(), workflowRun.getHeadBranch());
                    workflowRunToCancel.cancel();
                } else {
                    LOG.infof("Workflow run #%d - Cancelling workflow run #%d for branch '%s'",
                            workflowRun.getId(), workflowRunToCancel.getId(), workflowRun.getHeadBranch());
                }
            } catch (Exception e) {
                LOG.errorf(e, "Workflow run #%d - Unable to cancel workflow run #%d for branch '%s'",
                        workflowRun.getId(), workflowRunToCancel.getId(), workflowRun.getHeadBranch());
            }
        }
    }

    void rerunLastCancelledWorkflow(@WorkflowRun.Completed GHEventPayload.WorkflowRun workflowRunPayload) {
        GHWorkflowRun workflowRun = workflowRunPayload.getWorkflowRun();

        if (!GHEvent.PUSH.equals(workflowRun.getEvent())
                || GHWorkflowRun.Conclusion.CANCELLED == workflowRun.getConclusion()) {
            return;
        }

        Optional<GHWorkflowRun> lastFollowingCancelledRun = getFollowingCancelledRuns(workflowRun)
                .reduce((first, second) -> second);
        if (lastFollowingCancelledRun.isEmpty()) {
            LOG.tracef(
                    "Workflow run #%d - No following workflow run to rerun on branch '%s'",
                    (Object) workflowRun.getId(), workflowRun.getHeadBranch());
            return;
        }

        GHWorkflowRun workflowRunToRerun = lastFollowingCancelledRun.get();

        LOG.tracef(
                "Workflow run #%d - Will rerun workflow run #%d for branch '%s'"
                        + " because it was cancelled while this workflow run was running",
                workflowRun.getId(), workflowRunToRerun.getId(), workflowRun.getHeadBranch());

        try {
            if (!quarkusBotConfig.isDryRun()) {
                workflowRunToRerun.rerun();
            } else {
                LOG.infof("Workflow run #%d - Rerunning workflow run #%d for branch '%s'",
                        workflowRun.getId(), workflowRunToRerun.getId(), workflowRun.getHeadBranch());
            }
        } catch (Exception e) {
            LOG.errorf(e, "Workflow run #%d - Unable to rerun workflow run #%d for branch '%s'",
                    workflowRun.getId(), workflowRunToRerun.getId(), workflowRun.getHeadBranch());
        }
    }

    private static Stream<GHWorkflowRun> getPreviousIncompleteRuns(GHWorkflowRun workflowRun) {
        return Stream.concat(getSimilarRuns(workflowRun, Status.IN_PROGRESS),
                getSimilarRuns(workflowRun, Status.QUEUED))
                .filter(wr -> wr.getId() < workflowRun.getId())
                .sorted(Comparator.comparingLong(GHObject::getId));
    }

    private static Stream<GHWorkflowRun> getFollowingCancelledRuns(GHWorkflowRun workflowRun) {
        return getSimilarRuns(workflowRun, Status.COMPLETED)
                .filter(wr -> wr.getId() > workflowRun.getId())
                .filter(wr -> wr.getConclusion() == GHWorkflowRun.Conclusion.CANCELLED)
                .sorted(Comparator.comparingLong(GHObject::getId));
    }

    private static Stream<GHWorkflowRun> getSimilarRuns(GHWorkflowRun workflowRun, Status status) {
        return StreamSupport.stream(
                workflowRun.getRepository()
                        .queryWorkflowRuns()
                        .branch(workflowRun.getHeadBranch())
                        .status(status)
                        .list().spliterator(),
                false)
                .filter(wr -> wr.getWorkflowId() == workflowRun.getWorkflowId())
                .filter(wr -> wr.getHeadRepository().getId() == workflowRun.getHeadRepository().getId())
                .filter(wr -> GHEvent.PUSH.name().equals(getEvent(wr)) || GHEvent.PULL_REQUEST.name().equals(getEvent(wr)))
                .sorted(Comparator.comparingLong(GHObject::getId));
    }

    private static String getEvent(GHWorkflowRun workflowRun) {
        try {
            return workflowRun.getEvent().name();
        } catch (Exception e) {
            return "_UNKNOWN_";
        }
    }
}
