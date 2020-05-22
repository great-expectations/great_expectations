import logging
import traceback

from ....core import ExpectationConfiguration, ExpectationValidationResult
from ...types import (
    CollapseContent,
    RenderedMarkdownContent,
    RenderedStringTemplateContent,
    TextContent,
)
from ..renderer import Renderer

logger = logging.getLogger(__name__)


class ContentBlockRenderer(Renderer):
    _rendered_component_type = TextContent
    _default_header = ""

    _default_content_block_styling = {"classes": ["col-12"]}

    _default_element_styling = {}

    @classmethod
    def validate_input(cls, render_object):
        pass

    @classmethod
    def render(cls, render_object, **kwargs):
        cls.validate_input(render_object)

        data_docs_exception_message = f"""\
An unexpected Exception occurred during data docs rendering.  Because of this error, certain parts of data docs will \
not be rendered properly and/or may not appear altogether.  Please use the trace, included in this message, to \
diagnose and repair the underlying issue.  Detailed information follows:
        """

        if isinstance(render_object, list):
            blocks = []
            has_failed_evr = (
                False
                if isinstance(render_object[0], ExpectationValidationResult)
                else None
            )
            for obj_ in render_object:
                expectation_type = cls._get_expectation_type(obj_)

                content_block_fn = cls._get_content_block_fn(expectation_type)

                if isinstance(obj_, ExpectationValidationResult) and not obj_.success:
                    has_failed_evr = True

                if content_block_fn is not None:
                    try:
                        result = content_block_fn(
                            obj_, styling=cls._get_element_styling(), **kwargs
                        )
                    except Exception as e:
                        exception_traceback = traceback.format_exc()
                        exception_message = (
                            data_docs_exception_message
                            + f'{type(e).__name__}: "{str(e)}".  Traceback: "{exception_traceback}".'
                        )
                        logger.error(exception_message, e, exc_info=True)

                        if isinstance(obj_, ExpectationValidationResult):
                            content_block_fn = cls._get_content_block_fn(
                                "_missing_content_block_fn"
                            )
                        else:
                            content_block_fn = cls._missing_content_block_fn
                        result = content_block_fn(
                            obj_, cls._get_element_styling(), **kwargs
                        )
                else:
                    result = cls._missing_content_block_fn(
                        obj_, cls._get_element_styling(), **kwargs
                    )

                if result is not None:
                    if isinstance(obj_, ExpectationConfiguration):
                        expectation_meta_notes = cls._render_expectation_meta_notes(
                            obj_
                        )
                        if expectation_meta_notes:
                            # this adds collapse content block to expectation string
                            result[0] = [result[0], expectation_meta_notes]

                        horizontal_rule = RenderedStringTemplateContent(
                            **{
                                "content_block_type": "string_template",
                                "string_template": {
                                    "template": "",
                                    "tag": "hr",
                                    "styling": {"classes": ["mt-1", "mb-1"],},
                                },
                                "styling": {
                                    "parent": {"styles": {"list-style-type": "none"}}
                                },
                            }
                        )
                        result.append(horizontal_rule)

                    blocks += result

            if len(blocks) > 0:
                rendered_component_type_init_kwargs = {
                    cls._content_block_type: blocks,
                    "styling": cls._get_content_block_styling(),
                }
                rendered_component_type_default_init_kwargs = getattr(
                    cls, "_rendered_component_default_init_kwargs", {}
                )
                rendered_component_type_init_kwargs.update(
                    rendered_component_type_default_init_kwargs
                )
                content_block = cls._rendered_component_type(
                    **rendered_component_type_init_kwargs
                )
                cls._process_content_block(content_block, has_failed_evr=has_failed_evr)

                return content_block
            else:
                return None
        else:
            expectation_type = cls._get_expectation_type(render_object)

            content_block_fn = getattr(cls, expectation_type, None)
            if content_block_fn is not None:
                try:
                    result = content_block_fn(
                        render_object, styling=cls._get_element_styling(), **kwargs
                    )
                except Exception as e:
                    exception_traceback = traceback.format_exc()
                    exception_message = (
                        data_docs_exception_message
                        + f'{type(e).__name__}: "{str(e)}".  Traceback: "{exception_traceback}".'
                    )
                    logger.error(exception_message, e, exc_info=True)

                    if isinstance(render_object, ExpectationValidationResult):
                        content_block_fn = cls._get_content_block_fn(
                            "_missing_content_block_fn"
                        )
                    else:
                        content_block_fn = cls._missing_content_block_fn
                    result = content_block_fn(
                        render_object, cls._get_element_styling(), **kwargs
                    )
            else:
                result = cls._missing_content_block_fn(
                    render_object, cls._get_element_styling(), **kwargs
                )
            if result is not None:
                if isinstance(render_object, ExpectationConfiguration):
                    expectation_meta_notes = cls._render_expectation_meta_notes(
                        render_object
                    )
                    if expectation_meta_notes:
                        result.append(expectation_meta_notes)
            return result

    @classmethod
    def _render_expectation_meta_notes(cls, expectation):
        if not expectation.meta.get("notes"):
            return None
        else:
            collapse_link = RenderedStringTemplateContent(
                **{
                    "content_block_type": "string_template",
                    "string_template": {
                        "template": "$icon",
                        "params": {"icon": ""},
                        "styling": {
                            "params": {
                                "icon": {
                                    "classes": ["fas", "fa-comment", "text-info"],
                                    "tag": "i",
                                }
                            }
                        },
                    },
                }
            )
            notes = expectation.meta["notes"]
            note_content = None

            if isinstance(notes, str):
                note_content = [notes]

            elif isinstance(notes, list):
                note_content = notes

            elif isinstance(notes, dict):
                if "format" in notes:
                    if notes["format"] == "string":
                        if isinstance(notes["content"], str):
                            note_content = [notes["content"]]
                        elif isinstance(notes["content"], list):
                            note_content = notes["content"]
                        else:
                            logger.warning(
                                "Unrecognized Expectation suite notes format. Skipping rendering."
                            )

                    elif notes["format"] == "markdown":
                        if isinstance(notes["content"], str):
                            note_content = [
                                RenderedMarkdownContent(
                                    **{
                                        "content_block_type": "markdown",
                                        "markdown": notes["content"],
                                        "styling": {
                                            "parent": {"styles": {"color": "red"}}
                                        },
                                    }
                                )
                            ]
                        elif isinstance(notes["content"], list):
                            note_content = [
                                RenderedMarkdownContent(
                                    **{
                                        "content_block_type": "markdown",
                                        "markdown": note,
                                        "styling": {"parent": {}},
                                    }
                                )
                                for note in notes["content"]
                            ]
                        else:
                            logger.warning(
                                "Unrecognized Expectation suite notes format. Skipping rendering."
                            )
                else:
                    logger.warning(
                        "Unrecognized Expectation suite notes format. Skipping rendering."
                    )

            notes_block = TextContent(
                **{
                    "content_block_type": "text",
                    "subheader": "Notes:",
                    "text": note_content,
                    "styling": {
                        "classes": ["col-12", "mt-2", "mb-2"],
                        "parent": {"styles": {"list-style-type": "none"}},
                    },
                }
            )

            return CollapseContent(
                **{
                    "collapse_toggle_link": collapse_link,
                    "collapse": [notes_block],
                    "inline_link": True,
                    "styling": {
                        "body": {"classes": ["card", "card-body", "p-1"]},
                        "parent": {"styles": {"list-style-type": "none"}},
                    },
                }
            )

    @classmethod
    def _process_content_block(cls, content_block, has_failed_evr):
        header = cls._get_header()
        if header != "":
            content_block.header = header

    @classmethod
    def _get_content_block_fn(cls, expectation_type):
        return getattr(cls, expectation_type, None)

    @classmethod
    def list_available_expectations(cls):
        expectations = [attr for attr in dir(cls) if attr[:7] == "expect_"]
        return expectations

    @classmethod
    def _missing_content_block_fn(cls, obj, styling, **kwargs):
        return []

    @classmethod
    def _get_content_block_styling(cls):
        return cls._default_content_block_styling

    @classmethod
    def _get_element_styling(cls):
        return cls._default_element_styling

    @classmethod
    def _get_header(cls):
        return cls._default_header
