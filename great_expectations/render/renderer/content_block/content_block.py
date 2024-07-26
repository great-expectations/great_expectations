from __future__ import annotations

import logging
import traceback
from typing import TYPE_CHECKING, Any, Callable, Dict, Optional, Type, Union

from great_expectations.core import (
    ExpectationValidationResult,
)
from great_expectations.expectations.expectation_configuration import (
    ExpectationConfiguration,
)
from great_expectations.expectations.registry import (
    _registered_renderers,
    get_renderer_impl,
)
from great_expectations.render import (
    CollapseContent,
    LegacyRendererType,
    RenderedComponentContent,
    RenderedMarkdownContent,
    RenderedStringTemplateContent,
    TextContent,
)
from great_expectations.render.renderer.renderer import Renderer

if TYPE_CHECKING:
    from great_expectations.alias_types import JSONValues

logger = logging.getLogger(__name__)


class ContentBlockRenderer(Renderer):
    _rendered_component_type: Type[RenderedComponentContent] = TextContent
    _default_header = ""

    _default_content_block_styling: Dict[str, JSONValues] = {"classes": ["col-12"]}

    _default_element_styling = {}

    @classmethod
    def validate_input(cls, render_object: Any) -> None:
        pass

    @classmethod
    def render(cls, render_object: Any, **kwargs) -> Union[_rendered_component_type, Any, None]:
        cls.validate_input(render_object)
        exception_list_content_block: bool = kwargs.get("exception_list_content_block", False)

        data_docs_exception_message = """\
An unexpected Exception occurred during data docs rendering.  Because of this error, certain parts of data docs will \
not be rendered properly and/or may not appear altogether.  Please use the trace, included in this message, to \
diagnose and repair the underlying issue.  Detailed information follows:
        """  # noqa: E501

        runtime_configuration = {
            "styling": cls._get_element_styling(),
            "include_column_name": kwargs.pop("include_column_name", None),
        }

        # The specific way we render the render_object is contingent on the type of the object
        render_fn: Callable
        if isinstance(render_object, list):
            render_fn = cls._render_list
        else:
            render_fn = cls._render_other

        result = render_fn(
            render_object,
            exception_list_content_block,
            runtime_configuration,
            data_docs_exception_message,
            kwargs,
        )
        return result

    @classmethod
    def _get_content_block_fn_from_render_object(
        cls, obj_: ExpectationConfiguration | ExpectationValidationResult
    ):
        expectation_type = cls._get_expectation_type(obj_)
        expectation_config = (
            obj_.expectation_config if isinstance(obj_, ExpectationValidationResult) else obj_
        )

        return cls._get_content_block_fn(
            expectation_type=expectation_type, expectation_config=expectation_config
        )

    @classmethod
    def _render_list(  # noqa: C901, PLR0912
        cls,
        render_object: list,
        exception_list_content_block: bool,
        runtime_configuration: dict,
        data_docs_exception_message: str,
        kwargs: dict,
    ) -> Optional[_rendered_component_type]:
        """Helper method to render list render_objects - refer to `render` for more context"""
        blocks = []
        has_failed_evr = (
            False if isinstance(render_object[0], ExpectationValidationResult) else None
        )
        for obj_ in render_object:
            content_block_fn = cls._get_content_block_fn_from_render_object(obj_)

            if isinstance(obj_, ExpectationValidationResult) and not obj_.success:
                has_failed_evr = True

            if content_block_fn is not None and not exception_list_content_block:
                try:
                    if isinstance(obj_, ExpectationValidationResult):
                        expectation_config = obj_.expectation_config
                        result = content_block_fn(
                            configuration=expectation_config,
                            result=obj_,
                            runtime_configuration=runtime_configuration,
                            **kwargs,
                        )
                    else:
                        result = content_block_fn(
                            configuration=obj_,
                            runtime_configuration=runtime_configuration,
                            **kwargs,
                        )
                except Exception as e:
                    exception_traceback = traceback.format_exc()
                    exception_message = (
                        data_docs_exception_message
                        + f'{type(e).__name__}: "{e!s}".  Traceback: "{exception_traceback}".'
                    )
                    logger.error(exception_message)  # noqa: TRY400

                    if isinstance(obj_, ExpectationValidationResult):
                        content_block_fn = cls._get_content_block_fn("_missing_content_block_fn")
                        expectation_config = obj_.expectation_config
                        result = content_block_fn(
                            configuration=expectation_config,
                            result=obj_,
                            runtime_configuration=runtime_configuration,
                            **kwargs,
                        )
                    else:
                        content_block_fn = cls._missing_content_block_fn
                        result = content_block_fn(
                            configuration=obj_,
                            runtime_configuration=runtime_configuration,
                            **kwargs,
                        )
            else:  # noqa: PLR5501
                if isinstance(obj_, ExpectationValidationResult):
                    content_block_fn = (
                        cls._missing_content_block_fn
                        if exception_list_content_block
                        else cls._get_content_block_fn("_missing_content_block_fn")
                    )
                    expectation_config = obj_.expectation_config
                    result = content_block_fn(
                        configuration=expectation_config,
                        result=obj_,
                        runtime_configuration=runtime_configuration,
                        **kwargs,
                    )
                else:
                    content_block_fn = cls._missing_content_block_fn
                    result = content_block_fn(
                        configuration=obj_,
                        runtime_configuration=runtime_configuration,
                        **kwargs,
                    )

            if result is not None:
                if isinstance(obj_, ExpectationConfiguration):
                    expectation_notes = cls._render_expectation_notes(obj_)
                    if expectation_notes:
                        # this adds collapse content block to expectation string
                        result[0] = [result[0], expectation_notes]

                    horizontal_rule = RenderedStringTemplateContent(
                        **{
                            "content_block_type": "string_template",
                            "string_template": {
                                "template": "",
                                "tag": "hr",
                                "styling": {
                                    "classes": ["mt-1", "mb-1"],
                                },
                            },
                            "styling": {"parent": {"styles": {"list-style-type": "none"}}},
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
            rendered_component_type_init_kwargs.update(rendered_component_type_default_init_kwargs)
            content_block = cls._rendered_component_type(**rendered_component_type_init_kwargs)
            cls._process_content_block(
                content_block,
                has_failed_evr=has_failed_evr,
                render_object=render_object,
            )

            return content_block
        else:
            return None

    @classmethod
    def _render_other(  # noqa: C901
        cls,
        render_object: Any,
        exception_list_content_block: bool,
        runtime_configuration: dict,
        data_docs_exception_message: str,
        kwargs: dict,
    ) -> Any:
        """Helper method to render non-list render_objects - refer to `render` for more context"""
        content_block_fn = cls._get_content_block_fn_from_render_object(render_object)
        if content_block_fn is not None and not exception_list_content_block:
            try:
                if isinstance(render_object, ExpectationValidationResult):
                    result = content_block_fn(
                        result=render_object,
                        runtime_configuration=runtime_configuration,
                        **kwargs,
                    )
                else:
                    result = content_block_fn(
                        configuration=render_object,
                        runtime_configuration=runtime_configuration,
                        **kwargs,
                    )
            except Exception as e:
                exception_traceback = traceback.format_exc()
                exception_message = (
                    data_docs_exception_message
                    + f'{type(e).__name__}: "{e!s}".  Traceback: "{exception_traceback}".'
                )
                logger.error(exception_message)  # noqa: TRY400

                if isinstance(render_object, ExpectationValidationResult):
                    content_block_fn = cls._get_content_block_fn("_missing_content_block_fn")
                    result = content_block_fn(
                        result=render_object,
                        runtime_configuration=runtime_configuration,
                        **kwargs,
                    )
                else:
                    content_block_fn = cls._missing_content_block_fn
                    result = content_block_fn(
                        configuration=render_object,
                        runtime_configuration=runtime_configuration,
                        **kwargs,
                    )
        else:  # noqa: PLR5501
            if isinstance(render_object, ExpectationValidationResult):
                content_block_fn = (
                    cls._missing_content_block_fn
                    if exception_list_content_block
                    else cls._get_content_block_fn("_missing_content_block_fn")
                )
                result = content_block_fn(
                    result=render_object,
                    runtime_configuration=runtime_configuration,
                    **kwargs,
                )
            else:
                content_block_fn = cls._missing_content_block_fn
                result = content_block_fn(
                    configuration=render_object,
                    runtime_configuration=runtime_configuration,
                    **kwargs,
                )
        if result is not None:
            if isinstance(render_object, ExpectationConfiguration):
                expectation_notes = cls._render_expectation_notes(render_object)
                if expectation_notes:
                    result.append(expectation_notes)
        return result

    @classmethod
    def _render_expectation_description(
        cls,
        configuration: ExpectationConfiguration,
        runtime_configuration: dict,
        **kwargs,
    ) -> list[RenderedStringTemplateContent]:
        expectation = configuration.to_domain_obj()
        description = expectation.description
        if not description:
            raise ValueError("Cannot render an expectation with no description.")  # noqa: TRY003
        # If we wish to support $VAR substitution, we should use RenderedStringTemplateContent with params  # noqa: E501
        return [
            RenderedMarkdownContent(
                markdown=description, styling=runtime_configuration.get("styling", {})
            )
        ]

    @classmethod
    def _render_expectation_notes(
        cls, expectation_config: ExpectationConfiguration
    ) -> CollapseContent:
        notes = expectation_config.notes
        if not notes:
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

            if isinstance(notes, str):
                note_content = [
                    RenderedMarkdownContent(
                        **{
                            "content_block_type": "markdown",
                            "markdown": notes,
                            "styling": {"parent": {"styles": {"color": "red"}}},
                        }
                    )
                ]
            elif isinstance(notes, list):
                note_content = [
                    RenderedMarkdownContent(
                        **{
                            "content_block_type": "markdown",
                            "markdown": note,
                            "styling": {"parent": {}},
                        }
                    )
                    for note in notes
                ]
            else:
                note_content = None

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
    def _process_content_block(cls, content_block, has_failed_evr, render_object=None) -> None:
        header = cls._get_header()
        if header != "":
            content_block.header = header

    @classmethod
    def _get_content_block_fn(
        cls,
        expectation_type: str,
        expectation_config: ExpectationConfiguration | None = None,
    ) -> Callable | None:
        # Prioritize `description` param on Expectation before falling back to renderer
        if expectation_config:
            content_block_fn = cls._get_content_block_fn_from_expectation_description(
                expectation_config=expectation_config,
            )
            if content_block_fn:
                return content_block_fn

        content_block_fn = get_renderer_impl(
            object_name=expectation_type, renderer_type=LegacyRendererType.PRESCRIPTIVE
        )
        return content_block_fn[1] if content_block_fn else None

    @classmethod
    def _get_content_block_fn_from_expectation_description(
        cls, expectation_config: ExpectationConfiguration
    ) -> Callable | None:
        expectation = expectation_config.to_domain_obj()
        description = expectation.description
        if description:
            return cls._render_expectation_description
        return None

    @classmethod
    def list_available_expectations(cls):
        expectations = [
            object_name
            for object_name in _registered_renderers
            if object_name.startswith("expect_")
        ]
        return expectations

    @classmethod
    def _missing_content_block_fn(
        cls,
        configuration: Optional[ExpectationConfiguration] = None,
        result: Optional[ExpectationValidationResult] = None,
        runtime_configuration: Optional[dict] = None,
        **kwargs,
    ):
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
