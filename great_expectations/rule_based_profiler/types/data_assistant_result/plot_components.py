
from dataclasses import dataclass
from typing import Optional, Union
import altair as alt

@dataclass(frozen=True)
class PlotComponent():
    name: str
    alt_type: alt.StandardType

    @property
    def title(self) -> str:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        return self.name.replace('_', ' ').title()

    def generate_tooltip(self, format: str='') -> alt.Tooltip:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'Wrapper arount alt.Tooltip creation.\n\n        Args:\n            format (str): Desired format within tooltip\n\n        Returns:\n            An instance of alt.Tooltip containing relevant information from the PlotComponent class.\n        '
        return alt.Tooltip(field=self.name, type=self.alt_type, title=self.title, format=format)

    def plot_on_axis(self) -> Union[(alt.X, alt.Y)]:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'Wrapper around alt.X/alt.Y plotting utility.\n\n        Returns:\n            Either an alt.X or alt.Y instance based on desired axis.\n        '
        raise NotImplementedError

@dataclass(frozen=True)
class MetricPlotComponent(PlotComponent):

    def plot_on_axis(self) -> alt.Y:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        '\n        Plots metric on Y axis - see parent `PlotComponent` for more details.\n        '
        return alt.Y(self.name, type=self.alt_type, title=self.title)

@dataclass(frozen=True)
class DomainPlotComponent(PlotComponent):
    subtitle: Optional[str] = None

    @property
    def title(self) -> str:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        return self.name.title()

    def plot_on_axis(self) -> alt.X:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        '\n        Plots domain on X axis - see parent `PlotComponent` for more details.\n        '
        return alt.X(self.name, type=self.alt_type, title=self.title)

@dataclass(frozen=True)
class BatchPlotComponent(PlotComponent):

    @property
    def title(self) -> str:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        return self.name.replace('_', ' ').title().replace('Id', 'ID')

@dataclass(frozen=True)
class ExpectationKwargPlotComponent(PlotComponent):
    metric_plot_component: MetricPlotComponent

    def plot_on_axis(self) -> alt.Y:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        '\n        Plots domain on Y axis - see parent `PlotComponent` for more details.\n        '
        return alt.Y(self.name, type=self.metric_plot_component.alt_type, title=self.metric_plot_component.title)

def determine_plot_title(metric_plot_component: MetricPlotComponent, domain_plot_component: DomainPlotComponent) -> alt.TitleParams:
    import inspect
    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
            continue
        print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
    'Determines the appropriate title for a chart based on input componentsself.\n\n    Conditionally renders a subtitle if relevant (specifically with column domain)\n\n    Args:\n        metric_plot_component: Plot utility corresponding to a given metric.\n        domain_plot_component: Plot utility corresponding to a given domain.\n\n    Returns:\n        An Altair TitleParam object\n\n    '
    contents: str = f'{metric_plot_component.title} per {domain_plot_component.title}'
    subtitle: Optional[str] = domain_plot_component.subtitle
    title: alt.TitleParams
    if subtitle:
        title = alt.TitleParams(contents, subtitle=[subtitle])
    else:
        title = alt.TitleParams(contents)
    return title
