from dataclasses import dataclass
from typing import Optional, Union

import altair as alt


@dataclass(frozen=True)
class PlotComponent:
    name: str
    alt_type: alt.StandardType

    @property
    def title(self) -> str:
        return self.name.replace("_", " ").title()

    def generate_tooltip(self, format: str = "") -> alt.Tooltip:
        """Wrapper arount alt.Tooltip creation.

        Args:
            format (str): Desired format within tooltip

        Returns:
            An instance of alt.Tooltip containing relevant information from the PlotComponent class.
        """
        return alt.Tooltip(
            field=self.name,
            type=self.alt_type,
            title=self.title,
            format=format,
        )

    def plot_on_axis(self, shorthand: Optional[str] = None) -> Union[alt.X, alt.Y]:
        """Wrapper around alt.X/alt.Y plotting utility.

        Args:
            shorthand (Optional[str]): If provided, used as shorthand for field, aggregate, and type.

        Returns:
            Either an alt.X or alt.Y instance based on desired axis.
        """
        raise NotImplementedError


@dataclass(frozen=True)
class MetricPlotComponent(PlotComponent):
    def plot_on_axis(self, shorthand: Optional[str] = None) -> alt.Y:
        """
        Plots metric on Y axis - see parent `PlotComponent` for more details.
        """
        if shorthand is None:
            shorthand = self.name
        return alt.Y(
            shorthand,
            type=self.alt_type,
            title=self.title,
        )


@dataclass(frozen=True)
class DomainPlotComponent(PlotComponent):
    subtitle: Optional[str] = None

    @property
    def title(self) -> str:
        return self.name.title()

    def plot_on_axis(self, shorthand: Optional[str] = None) -> alt.X:
        """
        Plots domain on X axis - see parent `PlotComponent` for more details.
        """
        if shorthand is None:
            shorthand = self.name
        return alt.X(
            shorthand,
            type=self.alt_type,
            title=self.title,
        )


@dataclass(frozen=True)
class BatchPlotComponent(PlotComponent):
    @property
    def title(self) -> str:
        return self.name.replace("_", " ").title().replace("Id", "ID")


def determine_plot_title(
    metric_plot_component: MetricPlotComponent,
    domain_plot_component: DomainPlotComponent,
) -> alt.TitleParams:
    """Determines the appropriate title for a chart based on input componentsself.

    Conditionally renders a subtitle if relevant (specifically with column domain)

    Args:
        metric_plot_component: Plot utility corresponding to a given metric.
        domain_plot_component: Plot utility corresponding to a given domain.

    Returns:
        An Altair TitleParam object

    """
    contents: str = f"{metric_plot_component.title} per {domain_plot_component.title}"
    subtitle: Optional[str] = domain_plot_component.subtitle

    title: alt.TitleParams
    if subtitle:
        title = alt.TitleParams(contents, subtitle=[subtitle])
    else:
        title = alt.TitleParams(contents)

    return title
