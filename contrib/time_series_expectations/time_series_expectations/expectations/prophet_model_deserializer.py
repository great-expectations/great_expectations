# Suppress an annoying warning from Prophet.
import logging

logger = logging.getLogger("prophet.plot")
logger.setLevel(logging.CRITICAL)
try:
    from prophet import Prophet
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
