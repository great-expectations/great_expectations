import os
import json
import uuid

import click
from humbug.consent import HumbugConsent, environment_variable_opt_out
from humbug.report import Reporter

from great_expectations import __version__ as ge_version

HUMBUG_TOKEN = "e10fbd54-71b0-4e68-80b0-d59ec3d99a81"
HUMBUG_KB_ID = "2e995d6c-95a9-4a35-8ee6-49846ac7fc63"
GE_REPORTING_CONFIG_FILE_NAME = "reporting_config.json"
GE_REPORTING_CONFIG_DIR = os.path.expanduser("~/.greate_expectations")



def get_reporting_config_path():
    return os.path.join(GE_REPORTING_CONFIG_DIR, GE_REPORTING_CONFIG_FILE_NAME)

def chek_config_path():
    if not os.path.exists(GE_REPORTING_CONFIG_DIR):
        os.makedirs(GE_REPORTING_CONFIG_DIR)
        return False
    return True



def save_reporting_config(consent: bool, client_id = None):
    """
    Allow or disallow Aim reporting.
    """
    config_report_path = get_reporting_config_path()
    if config_report_path is None:
        raise Exception(
            'Config report file not found, use "aim init" to initialize a new repository'
        )

    reporting_config = {}
    if os.path.isfile(config_report_path):
        try:
            with open(config_report_path, "r") as ifp:
                reporting_config = json.load(ifp)
        except Exception:
            pass

    if client_id is not None and reporting_config.get("client_id") is None:
        reporting_config["client_id"] = client_id

    if reporting_config.get("client_id") is None:
        reporting_config["client_id"] = str(uuid.uuid4())

    reporting_config["consent"] = consent

    try:
        with open(config_report_path, "w") as ofp:
            json.dump(reporting_config, ofp)
    except Exception:
        pass


def get_reporting_config():
    reporting_config = {}

    config_report_path = get_reporting_config_path()
    if config_report_path is not None:
        try:
            if not os.path.exists(config_report_path):
                client_id = str(uuid.uuid4())
                reporting_config["client_id"] = client_id
                save_reporting_config(True, client_id)
            else:
                with open(config_report_path, "r") as ifp:
                    reporting_config = json.load(ifp)
        except Exception:
            pass
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