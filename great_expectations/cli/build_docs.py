from great_expectations.cli import toolkit
from great_expectations.cli.cli_logging import logger
from great_expectations.cli.pretty_printing import cli_message


def build_docs(context, site_name=None, view=True, assume_yes=False):
    """Build documentation in a context"""
    logger.debug("Starting cli.datasource.build_docs")

    if site_name is not None:
        site_names = [site_name]
    else:
        site_names = None
    index_page_locator_infos = context.build_data_docs(
        site_names=site_names, dry_run=True
    )

    msg = "\nThe following Data Docs sites will be built:\n\n"
    for site_name, index_page_locator_info in index_page_locator_infos.items():
        msg += " - <cyan>{}:</cyan> ".format(site_name)
        msg += "{}\n".format(index_page_locator_info)

    cli_message(msg)
    if not assume_yes:
        toolkit.confirm_proceed_or_exit()

    cli_message("\nBuilding Data Docs...\n")
    context.build_data_docs(site_names=site_names)

    cli_message("Done building Data Docs")

    if view:
        context.open_data_docs(site_name=site_name, only_if_exists=True)
