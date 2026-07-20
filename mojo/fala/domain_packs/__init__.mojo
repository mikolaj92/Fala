"""Domain packs map external vocabularies onto core Fala records.

Packs must not become core identity — core stays domain-agnostic.
"""

from .splot import (
    SPLOT_DOMAIN_PACK_ID,
    SPLOT_ARBITRATION_CASE,
    SPLOT_JURISDICTION,
    SPLOT_REVIEW,
    SplotArbitrationCase,
    impulse_from_case,
    case_from_impulse,
    jurisdiction_association,
    review_homeostat,
    case_projection,
    process_semantics_json,
)
from .takt import (
    TAKT_DOMAIN_PACK_ID,
    TAKT_CASCADE_REQUEST,
    TAKT_PLANT_LAYER,
    TAKT_ERROR_SIGNAL,
    TAKT_SAFETY_INTERLOCK,
    TAKT_ACTUATION,
    TaktCascadeRequest,
    impulse_from_cascade,
    cascade_from_impulse,
    plant_layer_association,
    error_signal_association,
    safety_interlock_homeostat,
    cascade_projection,
)
