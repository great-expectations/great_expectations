# Suppress an annoying warning from Prophet.
from __future__ import annotations

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from prophet import Prophet

logger = logging.getLogger("prophet.plot")
logger.setLevel(logging.CRITICAL)
try:
    from prophet.serialize import model_from_json
except ImportError:
    logger.warning(
        "Unable to import Prophet. Please install prophet to use this Expectation."
    )
    model_from_json = None


class ProphetModelDeserializer:
    """A class for deserializing a Prophet model from JSON"""

    def get_model(
        self,
        model_json: str,
    ) -> Prophet:
        if not model_from_json:
            raise ImportError(
                "Unable to import Prophet. Please install prophet to use this Expectation."
            )

        return model_from_json(model_json)
