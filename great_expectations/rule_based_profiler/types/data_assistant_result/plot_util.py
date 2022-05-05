from dataclasses import dataclass
from typing import Optional, Union

import altair as alt


@dataclass(frozen=True)
class PlotUtil:
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
            An instance of alt.Tooltip containing relevant information from the PlotUtil class.
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
class MetricPlotUtil(PlotUtil):
    def plot_on_axis(self, shorthand: Optional[str] = None) -> alt.Y:
        """
        Plots metric on Y axis - see parenet `PlotUtil` for more details.
        """
        if shorthand is None:
            shorthand = self.name
        return alt.Y(
            shorthand,
            type=self.alt_type,
            title=self.title,
        )


@dataclass(frozen=True)
class DomainPlotUtil(PlotUtil):
    @property
    def title(self) -> str:
        return self.name.title()

    def plot_on_axis(self, shorthand: Optional[str] = None) -> alt.X:
        """
        Plots domain on X axis - see parenet `PlotUtil` for more details.
        """
        if shorthand is None:
            shorthand = self.name
        return alt.X(
            shorthand,
            type=self.alt_type,
            title=self.title,
        )


@dataclass(frozen=True)
class BatchIdPlotUtil(PlotUtil):
    @property
    def title(self) -> str:
        return self.name.replace("_", " ").title().replace("Id", "ID")
