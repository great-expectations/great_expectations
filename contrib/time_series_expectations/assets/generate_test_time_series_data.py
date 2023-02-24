"""Generate test data for time series. Includes both CSVs and PNGs"""

import os.path

import matplotlib.pyplot as plt
import numpy as np

from great_expectations.data_context.util import file_relative_path
from time_series_expectations.generator import generate_daily_time_series_df

plt.rcParams["figure.figsize"] = (20, 3)


def generate_time_series_data_and_plot(
    grain,
    size,
    intercept,
    # noise,
    trend,
    weekly_seasonality,
    # annual_seasonality,
    outlier_alpha,
):
    filename = f"{grain}__size_{size}__trend_{trend}__weekly_seasonality_{weekly_seasonality}__outliers_{outlier_alpha}"
    print(filename)

    df = generate_daily_time_series_df(
        size=size,
        trend_params=[
            {
                "alpha": intercept,
                "beta": trend,
                "cutpoint": size,
            }
        ],
        weekday_dummy_params=[
            weekly_seasonality * x for x in [0, 0.5, 3.5, 4, 5, -3, -3.5]
        ],
        annual_seasonality_params=[],
        holiday_alpha=1000,
        outlier_alpha=outlier_alpha,
    )

    df.to_csv(
        os.path.join(file_relative_path(__file__, "data"), f"{filename}.csv"),
        index=None,
    )

    plt.plot(df.y)
    plt.savefig(
        os.path.join(file_relative_path(__file__, "pics"), f"{filename}.png"),
    )
    plt.clf()


# Generate a bunch of test data: daily, 180 days long, with intercept 100
# These are good for showing the effect of different parameters

np.random.seed(7)

generate_time_series_data_and_plot(
    grain="daily",
    size=180,
    intercept=100,
    trend=0,
    weekly_seasonality=0,
    outlier_alpha=1000,
)

generate_time_series_data_and_plot(
    grain="daily",
    size=180,
    intercept=100,
    trend=0.05,
    weekly_seasonality=0,
    outlier_alpha=1000,
)

generate_time_series_data_and_plot(
    grain="daily",
    size=180,
    intercept=100,
    trend=0.05,
    weekly_seasonality=1,
    outlier_alpha=1000,
)

np.random.seed(7)

generate_time_series_data_and_plot(
    grain="daily",
    size=180,
    intercept=100,
    trend=0.05,
    weekly_seasonality=1,
    outlier_alpha=2,
)

# Generate a bunch of test data: daily, 3 years long, with intercept 100
# grain = "daily"
# # length = 365*3
# length = 180
# intercept = 100
# for noise, trend, weekly_seasonality, outlier_alpha in itertools.product(
#     [1, 5, 10, 25],
#     [0, .01, .03, .1],
#     [0, 1, 5, 10],
#     [10000, 10, 4, 2],
# ):
#     break

#     filename = f"{grain}__length_{length}__trend_{trend}__weekly_seasonality_{weekly_seasonality}__outliers_{outlier_alpha}"
#     print(filename)

#     np.random.seed(0)

#     df = generate_time_series_df(
#         size=length,
#         trend_params=[{
#             "alpha": intercept,
#             "beta": trend,
#             "cutpoint": length,
#         }],
#         weekday_dummy_params=[weekly_seasonality*x for x in [0, .5, 3.5, 4, 5, -3, -3.5]],
#         # weekday_dummy_params=[0 for i in range(7)],
#         annual_seasonality_params=[],
#         holiday_alpha=1000,
#         outlier_alpha=outlier_alpha,
#     )

#     df.to_csv(
#         os.path.join(
#             file_relative_path(
#                 __file__,
#                 "data"
#             ),
#             f"{filename}.csv"
#         ),
#         index=None
#     )

#     plt.plot(df.y)
#     plt.savefig(
#         os.path.join(
#             file_relative_path(
#                 __file__,
#                 "pics"
#             ),
#             f"{filename}.png"
#         ),
#     )
#     plt.clf()


# # Generate a bunch of test data: daily, 3 years long, with intercept 100
# grain = "daily"
# # length = 365*3
# length = 180
# intercept = 100
# for noise, trend, weekly_seasonality, outlier_alpha in itertools.product(
#     [1, 5, 10, 25],
#     [0, .01, .03, .1],
#     [0, 1, 5, 10],
#     [10000, 10, 4, 2],
# ):
#     filename = f"{grain}__length_{length}__trend_{trend}__weekly_seasonality_{weekly_seasonality}__outliers_{outlier_alpha}"
#     print(filename)

#     np.random.seed(0)

#     df = generate_time_series_df(
#         size=length,
#         trend_params=[{
#             "alpha": intercept,
#             "beta": trend,
#             "cutpoint": length,
#         }],
#         weekday_dummy_params=[weekly_seasonality*x for x in [0, .5, 3.5, 4, 5, -3, -3.5]],
#         # weekday_dummy_params=[0 for i in range(7)],
#         annual_seasonality_params=[],
#         holiday_alpha=1000,
#         outlier_alpha=outlier_alpha,
#     )

#     df.to_csv(
#         os.path.join(
#             file_relative_path(
#                 __file__,
#                 "data"
#             ),
#             f"{filename}.csv"
#         ),
#         index=None
#     )

#     plt.plot(df.y)
#     plt.savefig(
#         os.path.join(
#             file_relative_path(
#                 __file__,
#                 "pics"
#             ),
#             f"{filename}.png"
#         ),
#     )
#     plt.clf()
