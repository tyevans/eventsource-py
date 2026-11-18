"""ProjectionRebuildError is importable from every public front door."""

import eventsource
import eventsource.application.projections as projections
from eventsource.application.projections.coordinator import (
    ProjectionRebuildError as FromCoordinator,
)
from eventsource.application.projections.coordinator_projection import (
    ProjectionRebuildError as FromDefiningModule,
)


def test_rebuild_error_is_exported_from_projections_package() -> None:
    assert projections.ProjectionRebuildError is FromDefiningModule
    assert "ProjectionRebuildError" in projections.__all__


def test_rebuild_error_is_reexported_from_coordinator_module() -> None:
    assert FromCoordinator is FromDefiningModule


def test_rebuild_error_is_exported_from_top_level_package() -> None:
    assert eventsource.ProjectionRebuildError is FromDefiningModule
    assert "ProjectionRebuildError" in eventsource.__all__
