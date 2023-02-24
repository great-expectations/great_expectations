import pandas as pd
from prophet import Prophet
from prophet.serialize import model_to_json

df = pd.read_csv("data/daily__size_180__trend_0__weekly_seasonality_0__outliers_1000.csv")

m = Prophet(
    growth="flat",
    changepoints=[],
    daily_seasonality=False,
    weekly_seasonality=False,
    yearly_seasonality=False,
    interval_width=0.99,
)
m.fit(df)

model_json = model_to_json(m)
with open("models/example_prophet_date_model.json", "w") as f_:
    f_.write(model_json)