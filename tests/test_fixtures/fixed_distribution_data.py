"""
Use this code to create the reproducible data in ./tests/test_sets/fixed_distributional_test_data.csv

The data should pass a kstest with the cdf parameter=distribution and a=0.05,
e.g. kstest(data_column, "distribution name", p-value=0.05) == True

"""
import numpy as np
from scipy import stats
import pandas as pd


def generate_data():
    # Code used to create reproducible and static test data

    # std_loc, std_scale = 0, 1
    norm_mean, norm_std = -2, 5
    beta_a, beta_b, beta_loc, beta_scale = 0.5, 10, 5, 11
    gamma_a, gamma_loc, gamma_scale = 2, 20, 3
    # poisson_lambda, poisson_loc = 8.2, 40
    uniform_loc, uniform_scale = -5, 11
    chi2_df, chi2_loc, chi2_scale = 30, 3, 5
    expon_loc, expon_scale = 4.2, 10

    np.random.seed(12345)
    fixed = pd.DataFrame({
        'norm': stats.norm.rvs(loc=norm_mean, scale=norm_std, size=500),
        'norm_std': stats.norm.rvs(size=500),
        'beta': stats.beta.rvs(a=beta_a, b=beta_b, loc=beta_loc, scale=beta_scale, size=500),
        #'beta_std': stats.beta.rvs(a=beta_a, b=beta_b, size=500),
        'gamma': stats.gamma.rvs(a=gamma_a, loc=gamma_loc, scale=gamma_scale, size=500),
        #'gamma_std': stats.gamma.rvs(a=gamma_a, size=500),
        #'poisson': stats.poisson.rvs(mu=poisson_lambda, loc=poisson_loc, size=500),
        #'poisson_std': stats.poisson.rvs(mu=poisson_lambda, size=500),
        'uniform': stats.uniform.rvs(loc=uniform_loc, scale=uniform_scale, size=500),
        #'uniform_std': stats.uniform.rvs(size=500),
        'chi2': stats.chi2.rvs(df=chi2_df, loc=chi2_loc, scale=chi2_scale, size=500),
        #'chi2_std': stats.chi2.rvs(df=chi2_df, size=500),
        'exponential': stats.expon.rvs(loc=expon_loc, scale=expon_scale, size=500),
        #'exponential_std': stats.expon.rvs(size=500)
    })

    # different seed for chi2
    np.random.seed(123456)
    fixed['chi2'] = stats.chi2.rvs(
        df=chi2_df, loc=chi2_loc, scale=chi2_scale, size=500)

    return fixed


if __name__ == "__main__":
    # Set precision we'll use:
    #precision = sys.float_info.dig
    #print("Setting pandas float_format to use " + str(precision) + " digits of precision.")

    df = generate_data()
    df.to_csv("../test_sets/fixed_distributional_test_dataset.csv",
              header=True, index=None)
    with open('../test_sets/fixed_distributional_test_dataset.json', 'a') as data_file:
        for column in list(df):
            data_file.write("\"" + str(column) + "\" : [")
            data_file.write(str(df.iloc[0][column]))
            for data_point in range(1, len(df[column])):
                data_file.write("," + str(df.iloc[data_point][column]))

            data_file.write("],\n")
    #df.to_csv('../test_sets/fixed_distributional_test_dataset.csv', index=None, header=True)
