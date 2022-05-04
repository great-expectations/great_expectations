from abc import ABC, abstractmethod, abstractproperty
from dataclasses import dataclass
from typing import Optional, Union

import altair as alt


class PlotUtil(ABC):
    @abstractproperty
    def title(self) -> str:
        pass

    @abstractmethod
    def generate_tooltip(self) -> alt.Tooltip:
        pass

    @abstractmethod
    def plot_on_target_axis(
        self, axis: Union[alt.X, alt.Y], shorthand: Optional[str] = None
    ) -> Union[alt.X, alt.Y]:
        pass


@dataclass(frozen=True)
class MetricPlotUtil(PlotUtil):
    metric_name: str
    metric_type: alt.StandardType

    @property
    def title(self) -> str:
        return self.metric_name.replace("_", " ").title()

    def generate_tooltip(self) -> alt.Tooltip:
        return alt.Tooltip(
            field=self.metric_name,
            type=self.metric_type,
            title=self.title,
            format=",",
        )

    def plot_on_target_axis(
        self, axis: Union[alt.X, alt.Y], shorthand: Optional[str] = None
    ) -> Union[alt.X, alt.Y]:
        if shorthand is None:
            shorthand = self.metric_name
        return axis(
            shorthand,
            type=self.metric_type,
            title=self.title,
        )


@dataclass(frozen=True)
class DomainPlotUtil(PlotUtil):
    domain_name: str
    domain_type: alt.StandardType

    @property
    def title(self) -> str:
        return self.domain_name.title()

    def generate_tooltip(self) -> alt.Tooltip:
        return alt.Tooltip(
            field=self.domain_name,
            type=self.domain_type,
            title=self.title,
            format=",",
        )

    def plot_on_target_axis(
        self, axis: Union[alt.X, alt.Y], shorthand: Optional[str] = None
    ) -> Union[alt.X, alt.Y]:
        if shorthand is None:
            shorthand = self.domain_name
        return axis(
            shorthand,
            type=self.domain_type,
            title=self.title,
        )


@dataclass(frozen=True)
class BatchIdPlotUtil(PlotUtil):
    batch_id: str
    batch_id_type: alt.StandardType

    @property
    def title(self) -> str:
        return self.batch_id.replace("_", " ").title().replace("Id", "ID")

    def generate_tooltip(self) -> alt.Tooltip:
        return alt.Tooltip(
            field=self.batch_id,
            type=self.batch_id_type,
            title=self.title,
        )

    def plot_on_target_axis(
        self, axis: Union[alt.X, alt.Y], shorthand: Optional[str] = None
    ) -> Union[alt.X, alt.Y]:
        raise NotImplementedError
