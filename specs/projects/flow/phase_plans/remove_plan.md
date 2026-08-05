---
status: complete
historical: true
superseded_by: ../plans/3-remove-coordinator.md
---

# Command/event/coordinator reduction

> Historical delivery record for the intermediate architecture. The command-only replacement is specified by `../plans/3-remove-coordinator.md`.

This vertical delivery implements `../plans/2-remove-plan.md`:

1. exact event gates were generalized to direct and staged commands;
2. monitor and fan-out examples migrated to the retained API;
3. the complete obsolete workflow scheduler and public surface were deleted;
4. dependency storage, worker result snapshots, skip propagation, and failure scopes were deleted;
5. fail-fast was reduced to preserve already-running settlements while cancelling new descendants;
6. baseline storage, replay, inspection, tests, and tooling moved to two modes and seven tables;
7. active product specifications and evidence were synchronized.

The change is deliberately pre-release and requires clean Flow schemas.
