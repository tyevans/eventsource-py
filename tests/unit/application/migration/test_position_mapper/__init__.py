"""Modular tests package for PositionMapper."""

from ._fixtures import PositionMapperTestBase, pos
from .test_position_mapper_init_and_record import (
    TestPositionMapperHelperMethods,
    TestPositionMapperInit,
    TestPositionMapperRecordMapping,
    TestPositionMapperRecordMappingsBatch,
)
from .test_position_mapper_nearest_and_workflows import (
    TestPositionMapperFindNearest,
    TestPositionMapperWorkflows,
)
from .test_position_mapper_translation import (
    TestPositionMapperTranslatePosition,
    TestPositionMapperTranslatePositionReverse,
    TestPositionMapperTranslatePositionsBatch,
    TestReverseTranslationResultDataclass,
    TestTranslationResultDataclass,
)

__all__ = [  # pragma: allowlist secret
    "PositionMapperTestBase",
    "TestPositionMapperFindNearest",
    "TestPositionMapperHelperMethods",
    "TestPositionMapperInit",
    "TestPositionMapperRecordMapping",
    "TestPositionMapperRecordMappingsBatch",
    "TestPositionMapperTranslatePosition",
    "TestPositionMapperTranslatePositionReverse",
    "TestPositionMapperTranslatePositionsBatch",
    "TestPositionMapperWorkflows",
    "TestReverseTranslationResultDataclass",
    "TestTranslationResultDataclass",
    "pos",
]
