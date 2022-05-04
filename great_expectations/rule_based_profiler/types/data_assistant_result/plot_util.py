from typing import Optional, Union

import altair as alt


class PlotUtil:
    def __init__(self, name: str, alt_type: alt.StandardType) -> None:
        self._name = name
        self._alt_type = alt_type

    @property
    def name(self) -> str:
        return self._name

    @property
    def alt_type(self) -> alt.StandardType:
        return self._alt_type

    @property
    def title(self) -> str:
        return self.name.replace("_", " ").title()

    def generate_tooltip(self, format: str = "") -> alt.Tooltip:
        return alt.Tooltip(
            field=self.name,
            type=self.alt_type,
            title=self.title,
            format=format,
        )

    def plot_on_axis(self, shorthand: Optional[str] = None) -> Union[alt.X, alt.Y]:
        raise NotImplementedError


class MetricPlotUtil(PlotUtil):
    def plot_on_axis(self, shorthand: Optional[str] = None) -> alt.Y:
        if shorthand is None:
            shorthand = self.name
        return alt.Y(
            shorthand,
            type=self.alt_type,
            title=self.title,
        )


class DomainPlotUtil(PlotUtil):
    @property
    def title(self) -> str:
        return self.name.title()

    def plot_on_axis(self, shorthand: Optional[str] = None) -> alt.X:
        if shorthand is None:
            shorthand = self.name
        return alt.X(
            shorthand,
            type=self.alt_type,
            title=self.title,
        )


class BatchIdPlotUtil(PlotUtil):
    @property
    def title(self) -> str:
        return self.name.replace("_", " ").title().replace("Id", "ID")
