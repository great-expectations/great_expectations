import os
import json
import uuid

import click
from humbug.consent import HumbugConsent, environment_variable_opt_out
from humbug.report import Reporter

from great_expectations import __version__ as ge_version

HUMBUG_TOKEN = "e10fbd54-71b0-4e68-80b0-d59ec3d99a81"
HUMBUG_KB_ID = "2e995d6c-95a9-4a35-8ee6-49846ac7fc63"
REPORT_CONSENT_ENV_VAR = "GREATE_EXPECTATION_REPORTING_ENABLED"
REPORT_CLIENT_ID = "GREATE_EXPECTATION_CLIENT_ID"


def save_reporting_config(consent, client_id = None):
    """
    Allow or disallow Aim reporting.
    """
    os.environ[REPORT_CONSENT_ENV_VAR] = consent


def get_reporting_config():
    reporting_config = {}

    try:
        if REPORT_CONSENT_ENV_VAR not in os.environ:
            os.environ[REPORT_CONSENT_ENV_VAR] = True
            if REPORT_CLIENT_ID not in os.environ:
                client_id = str(uuid.uuid4())
                os.environ[REPORT_CLIENT_ID] = client_id
        reporting_config["client_id"] = os.environ.get(REPORT_CLIENT_ID)
        reporting_config["consent"] = os.environ.get(REPORT_CONSENT_ENV_VAR)
    except Exception as err:
        print(err)
    return reporting_config

@click.command()
@click.option("--allow/--disallow", default=True)
def reporting(on: bool) -> None:
    """
    Enable or disable sending crash reports to Great Expectations team/superconductive.
    """
    report = Reporter(
        title="Consent change",
        tags=ge_reporter.system_tags(),
        content="Consent? `{}`".format(on),
    )
    ge_reporter.publish(report)
    save_reporting_config(on)


def ge_consent_from_reporting_config_file() -> bool:
    reporting_config = get_reporting_config()
    return reporting_config.get("consent", False)

client_id = None

client_id_tag = "client_id:{}".format(client_id)

session_id = str(uuid.uuid4())
session_id_tag = "session_id:{}".format(session_id)

version_tag = f"version:{ge_version}-dev-reporting"


ge_version_tag = "version:{}".format(ge_version)
ge_tags = [ge_version_tag]

session_id = str(uuid.uuid4())


client_id = get_reporting_config().get("client_id")

ge_consent = HumbugConsent(ge_consent_from_reporting_config_file)
ge_reporter = Reporter(
    name="great_expectation",
    consent=ge_consent,
    client_id=client_id,
    session_id=session_id,
    bugout_token=HUMBUG_TOKEN,
    bugout_journal_id=HUMBUG_KB_ID,
)