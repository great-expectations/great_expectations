from dataclasses import dataclass
from typing import Optional, Union

import altair as alt


@dataclass(frozen=True)
class PlotHelper:
    metric_name: str
    metric_type: alt.StandardType
    domain_name: str
    domain_type: alt.StandardType
    batch_id: str
    batch_id_type: alt.StandardType

    @property
    def metric_title(self) -> str:
        return self.metric_name.replace("_", " ").title()

    @property
    def domain_title(self) -> str:
        return self.domain_name.title()

    @property
    def batch_id_title(self) -> str:
        return self.batch_id.replace("_", " ").title().replace("Id", "ID")

    def get_batch_id_tooltip(self) -> alt.Tooltip:
        return alt.Tooltip(
            field=self.batch_id,
            type=self.batch_id_type,
            title=self.batch_id_title,
        )

    def get_domain_tooltip(self) -> alt.Tooltip:
        return alt.Tooltip(
            field=self.domain_name,
            type=self.domain_type,
            title=self.domain_title,
            format=",",
        )

    def get_metric_tooltip(self) -> alt.Tooltip:
        return alt.Tooltip(
            field=self.metric_name,
            type=self.metric_type,
            title=self.metric_title,
            format=",",
        )

    def plot_domain_on_target_axis(
        self, axis: Union[alt.X, alt.Y], shorthand: Optional[str] = None
    ) -> Union[alt.X, alt.Y]:
        if shorthand is None:
            shorthand = self.domain_name
        return axis(
            shorthand,
            type=self.domain_type,
            title=self.domain_title,
        )

    def plot_metric_on_target_axis(
        self, axis: Union[alt.X, alt.Y], shorthand: Optional[str] = None
    ) -> Union[alt.X, alt.Y]:
        if shorthand is None:
            shorthand = self.metric_name
        return axis(
            shorthand,
            type=self.metric_type,
            title=self.metric_title,
        )

    def determine_plot_title(self, subtitle: Optional[str]) -> alt.TitleParams:
        contents: str = f"{self.metric_title} per {self.domain_title}"

        title: alt.TitleParams
        if subtitle:
            title = alt.TitleParams(contents, subtitle=[subtitle])
        else:
            title = alt.TitleParams(contents)

        return title
