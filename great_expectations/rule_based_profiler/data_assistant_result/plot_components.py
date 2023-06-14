from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional, Sequence, Set, Union

import altair as alt


@dataclass(frozen=True)
class PlotComponent:
    alt_type: alt.StandardType
    name: str = ""
    axis_title: Optional[str] = None

    @property
    def title(self) -> Optional[str]:
        if self.axis_title is not None:
            return self.axis_title
        elif self.name is not None:
            return self.name.replace("_", " ").title()
        else:
            return None

    def plot_on_axis(self) -> Union[alt.X, alt.Y]:
        """Wrapper around alt.X/alt.Y plotting utility.

        Returns:
            Either an alt.X or alt.Y instance based on desired axis.
        """
        raise NotImplementedError

    def plot_on_x_axis(self) -> alt.X:
        """
        Plots domain on X axis.

        Returns:
            An instance of alt.X.
        """
        return alt.X(
            self.name,
            type=self.alt_type,
            title=self.title,
        )

    def plot_on_y_axis(self) -> alt.Y:
        """
        Plots domain on Y axis.

        Returns:
            An instance of alt.Y.
        """
        return alt.Y(
            self.name,
            type=self.alt_type,
            title=self.title,
        )

    def generate_tooltip(self, format: str = "") -> alt.Tooltip:
        """Wrapper around alt.Tooltip creation.

        Args:
            format (str): Desired format within tooltip

        Returns:
            An instance of alt.Tooltip containing relevant information from the PlotComponent class.
        """
        return alt.Tooltip(
            field=self.name,
            type=self.alt_type,
            title=self.name.replace("_", " ").title(),
            format=format,
        )


@dataclass(frozen=True)
class MetricPlotComponent(PlotComponent):
    def plot_on_axis(self) -> alt.Y:
        """
        Plots metric on Y axis - see parent `PlotComponent` for more details.
        """
        return alt.Y(
            self.name,
            type=self.alt_type,
            title=self.title,
        )

    def generate_tooltip(self, format: str = "") -> alt.Tooltip:
        """Wrapper around alt.Tooltip creation.

        Args:
            format (str): Desired format within tooltip

        Returns:
            An instance of alt.Tooltip containing relevant information from the PlotComponent class.
        """
        if self.name.endswith("s"):
            return alt.Tooltip(
                field=self.name,
                type=self.alt_type,
                title=self.name.replace("_", " ")[:-1].title(),
                format=format,
            )
        else:
            return alt.Tooltip(
                field=self.name,
                type=self.alt_type,
                title=self.name.replace("_", " ").title(),
                format=format,
            )


@dataclass(frozen=True)
class DomainPlotComponent(PlotComponent):
    subtitle: Optional[str] = None

    def plot_on_axis(self) -> alt.X:
        """
        Plots domain on X axis - see parent `PlotComponent` for more details.
        """
        return alt.X(
            self.name,
            type=self.alt_type,
            title=self.title,
        )


@dataclass(frozen=True)
class BatchPlotComponent(PlotComponent):
    batch_identifiers: Sequence[str] = ()

    @property
    def titles(self) -> List[str]:
        return [
            batch_identifier.replace("_", " ").title().replace("Id", "ID")
            for batch_identifier in self.batch_identifiers
        ]

    def plot_on_axis(self) -> alt.X:
        """
        Plots domain on X axis - see parent `PlotComponent` for more details.
        """
        return alt.X(
            self.name,
            type=self.alt_type,
            title=self.title,
        )

    def generate_tooltip(self, format: str = "") -> List[alt.Tooltip]:
        """Wrapper around alt.Tooltip creation.

        Args:
            format (str): Desired format within tooltip

        Returns:
            A list of instances of alt.Tooltip containing relevant information from the BatchPlotComponent class.
        """
        tooltip: List = []
        for batch_identifier, title in zip(self.batch_identifiers, self.titles):
            tooltip.append(
                alt.Tooltip(
                    field=batch_identifier,
                    type=self.alt_type,
                    title=title,
                    format=format,
                )
            )
        return tooltip


@dataclass(frozen=True)
class ExpectationKwargPlotComponent(PlotComponent):
    def plot_on_axis(self) -> alt.Y:
        """
        Plots metric on Y axis - see parent `PlotComponent` for more details.
        """
        return alt.Y(
            self.name,
            type=self.alt_type,
            title=self.title,
        )

    def generate_tooltip(self, format: str = "") -> alt.Tooltip:
        """Wrapper around alt.Tooltip creation.

        Args:
            format (str): Desired format within tooltip

        Returns:
            An instance of alt.Tooltip containing relevant information from the PlotComponent class.
        """
        if self.name.endswith("s"):
            return alt.Tooltip(
                field=self.name,
                type=self.alt_type,
                title=self.name.replace("_", " ")[:-1].title(),
                format=format,
            )
        else:
            return alt.Tooltip(
                field=self.name,
                type=self.alt_type,
                title=self.name.replace("_", " ").title(),
                format=format,
            )


def determine_plot_title(
    metric_plot_components: List[MetricPlotComponent],
    batch_plot_component: BatchPlotComponent,
    domain_plot_component: DomainPlotComponent,
    expectation_type: Optional[str] = None,
) -> alt.TitleParams:
    """Determines the appropriate title for a chart based on input components.

    Conditionally renders a subtitle if relevant (specifically with column domain)

    Args:
        metric_plot_components: A list of plot utilities corresponding to each metric.
        batch_plot_component: Plot utility corresponding to a given batch.
        domain_plot_component: Plot utility corresponding to a given domain.
        expectation_type: The name of the expectation.

    Returns:
        An Altair TitleParam object

    """
    metric_plot_component_titles: Set[str] = set()
    for metric_plot_component in metric_plot_components:
        if metric_plot_component.title:
            metric_plot_component_titles.add(metric_plot_component.title)

    contents: str
    if expectation_type:
        contents = expectation_type
    elif len(metric_plot_component_titles) == 1:
        contents = (
            f"{list(metric_plot_component_titles)[0]} per {batch_plot_component.title}"
        )
    else:
        contents = f"Column Values per {batch_plot_component.title}"

    subtitle: Optional[str] = domain_plot_component.subtitle
    domain_selector: Optional[str] = domain_plot_component.name

    title: alt.TitleParams
    if subtitle:
        title = alt.TitleParams(contents, subtitle=[subtitle], dy=-5)
    elif domain_selector:
        title = alt.TitleParams(contents, dy=-35)
    else:
        title = alt.TitleParams(contents)

    return title
