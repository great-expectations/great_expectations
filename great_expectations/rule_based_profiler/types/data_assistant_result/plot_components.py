from dataclasses import dataclass
from typing import List, Optional, Union

import altair as alt
import pandas as pd


@dataclass(frozen=True)
class PlotComponent:
    name: Optional[str] = None
    alt_type: Optional[alt.StandardType] = None

    @property
    def title(self) -> Optional[str]:
        if self.name is not None:
            return self.name.replace("_", " ").title()
        else:
            return None

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
            title=self.title,
            format=format,
        )

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
            scale=alt.Scale(align=0.05),
        )

    def plot_on_y_axis(self, df: pd.DataFrame) -> alt.Y:
        """
        Plots domain on Y axis.

        Returns:
            An instance of alt.Y.
        """
        raise NotImplementedError

    def plot_on_axis(self, df: Optional[pd.DataFrame] = None) -> Union[alt.X, alt.Y]:
        """Wrapper around alt.X/alt.Y plotting utility.

        Returns:
            Either an alt.X or alt.Y instance based on desired axis.
        """
        raise NotImplementedError


@dataclass(frozen=True)
class MetricPlotComponent(PlotComponent):
    def plot_on_y_axis(self, df: Optional[pd.DataFrame]) -> alt.Y:
        """
        Plots metric on Y axis - see parent `PlotComponent` for more details.
        """
        range_min: float = df[self.name].min()
        range_max: float = df[self.name].max()
        scale_min: float
        scale_max: float
        scale_min, scale_max = get_axis_scale_from_range(
            range_min=range_min, range_max=range_max
        )

        if df is not None:
            return alt.Y(
                self.name,
                type=self.alt_type,
                title=self.title,
                scale=alt.Scale(domain=[scale_min, scale_max]),
            )
        else:
            return alt.Y(
                self.name,
                type=self.alt_type,
                title=self.title,
            )


@dataclass(frozen=True)
class DomainPlotComponent(PlotComponent):
    subtitle: Optional[str] = None

    @property
    def title(self) -> Optional[str]:
        if self.name is not None:
            return self.name.title()
        else:
            return None

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
    batch_identifiers: Optional[List[str]] = None

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
            scale=alt.Scale(align=0.05),
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
    metric_plot_component: Optional[MetricPlotComponent] = None

    def plot_on_y_axis(self, df: pd.DataFrame) -> alt.Y:
        """
        Plots metric on Y axis - see parent `PlotComponent` for more details.
        """
        range_min: float = df[self.name].min()
        range_max: float = df[self.name].max()
        scale_min: float
        scale_max: float
        scale_min, scale_max = get_axis_scale_from_range(
            range_min=range_min, range_max=range_max
        )

        if df is not None:
            return alt.Y(
                self.name,
                type=self.alt_type,
                title=self.metric_plot_component.title,
                scale=alt.Scale(domain=[scale_min, scale_max]),
            )
        else:
            return alt.Y(
                self.name,
                type=self.alt_type,
                title=self.metric_plot_component.title,
            )


def get_axis_scale_from_range(
    range_min: float, range_max: float, added_range_pct: float = 0.01
):
    scale_range: float = range_max - range_min
    scale_min: float = range_min - (scale_range * added_range_pct)
    scale_max: float = range_max + (scale_range * added_range_pct)
    return scale_min, scale_max


def determine_plot_title(
    metric_plot_component: MetricPlotComponent,
    batch_plot_component: BatchPlotComponent,
    domain_plot_component: DomainPlotComponent,
    expectation_type: Optional[str] = None,
) -> alt.TitleParams:
    """Determines the appropriate title for a chart based on input components.

    Conditionally renders a subtitle if relevant (specifically with column domain)

    Args:
        metric_plot_component: Plot utility corresponding to a given metric.
        batch_plot_component: Plot utility corresponding to a given batch.
        domain_plot_component: Plot utility corresponding to a given domain.

    Returns:
        An Altair TitleParam object

    """
    contents: str
    if expectation_type:
        contents = expectation_type
    else:
        contents: str = (
            f"{metric_plot_component.title} per {batch_plot_component.title}"
        )
    subtitle: Optional[str] = domain_plot_component.subtitle
    domain_selector: Optional[str] = domain_plot_component.name

    title: alt.TitleParams
    if subtitle:
        title = alt.TitleParams(contents, subtitle=[subtitle])
    elif domain_selector:
        title = alt.TitleParams(contents, dy=-35)
    else:
        title = alt.TitleParams(contents)

    return title
