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
