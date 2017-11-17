import unittest
import json
import numpy as np

import great_expectations as ge


class TestDistributionalExpectations(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super(TestDistributionalExpectations, self).__init__(*args, **kwargs)
        self.D = ge.read_csv('./tests/test_sets/distributional_expectations_data_test.csv')

        with open('./tests/test_sets/test_partitions.json', 'r') as file:
            self.test_partitions = json.loads(file.read())

    def test_expect_column_chisquare_test_p_value_greater_than(self):
        T = [
                {
                    'args': ['categorical_fixed'],
                    'kwargs': {
                        'partition_object': self.test_partitions['categorical_fixed'],
                        'p': 0.05
                        },
                    'out': {'success': True, 'true_value': 1.}
                },
                {
                    'args': ['categorical_fixed'],
                    'kwargs': {
                        'partition_object': self.test_partitions['categorical_fixed_alternate'],
                        'p': 0.05
                    },
                    'out': {'success': False, 'true_value': 5.9032943409869654e-06}
                }
        ]
        for t in T:
            out = self.D.expect_column_chisquare_test_p_value_greater_than(*t['args'], **t['kwargs'])
            self.assertEqual(out['success'],t['out']['success'])
            self.assertEqual(out['true_value'], t['out']['true_value'])
            #out = self.D.expect_column_frequency_distribution_to_be(*t['args'], **t['kwargs'])
            #self.assertTrue(np.allclose(out['success'], t['out']['success']))
            #self.assertTrue(np.allclose(out['true_value'], t['out']['true_value']))

    def test_expect_column_kl_divergence_less_than_discrete(self):
        T = [
                {
                    'args': ['categorical_fixed'],
                    'kwargs': {
                        'partition_object': self.test_partitions['categorical_fixed'],
                        'threshold': 0.1
                        },
                    'out': {'success': True, 'true_value': 0.}
                },
                {
                    'args': ['categorical_fixed'],
                    'kwargs': {
                        'partition_object': self.test_partitions['categorical_fixed_alternate'],
                        'threshold': 0.1
                        },
                    'out': {'success': False, 'true_value': 0.12599700286677529}
                }
        ]
        for t in T:
            out = self.D.expect_column_kl_divergence_less_than(*t['args'], **t['kwargs'])
            self.assertTrue(np.allclose(out['success'], t['out']['success']))
            self.assertTrue(np.allclose(out['true_value'], t['out']['true_value']))

    def test_expect_column_bootrapped_ks_test_p_value_greater_than(self):
        T = [
                {
                    'args': ['norm_0_1'],
                    'kwargs':{'partition_object': self.test_partitions['norm_0_1_auto_no_holdout'], "p": 0.05},
                    'out':{'success':True, 'true_value': "RANDOMIZED"}
                },
                {
                    'args': ['norm_0_1'],
                    'kwargs': {'partition_object': self.test_partitions['norm_0_1_auto_tail_holdout'], "p": 0.05},
                    'out': {'success': True, 'true_value': "RANDOMIZED"}
                },
                {
                    'args': ['norm_0_1'],
                    'kwargs':{'partition_object': self.test_partitions['norm_0_1_uniform_no_holdout'], "p": 0.05},
                    'out':{'success':True, 'true_value': "RANDOMIZED"}
                },
                {
                    'args': ['norm_0_1'],
                    'kwargs':{'partition_object': self.test_partitions['norm_0_1_ntile_no_holdout'], "p": 0.05},
                    'out':{'success':True, 'true_value': "RANDOMIZED"}
                },
                {
                    'args': ['norm_0_1'],
                    'kwargs':{'partition_object': self.test_partitions['norm_0_1_kde_no_holdout'], "p": 0.05},
                    'out':{'success':True, 'true_value': "RANDOMIZED"}
                },
                {
                    'args': ['norm_1_1'],
                    'kwargs':{'partition_object': self.test_partitions['norm_0_1_auto_no_holdout'], "p": 0.05},
                    'out':{'success':False, 'true_value': "RANDOMIZED"}
                },
                {
                    'args': ['norm_1_1'],
                    'kwargs':{'partition_object': self.test_partitions['norm_0_1_uniform_no_holdout'], "p": 0.05},
                    'out':{'success':False, 'true_value': "RANDOMIZED"}
                },
                {
                    'args': ['norm_1_1'],
                    'kwargs':{'partition_object': self.test_partitions['norm_0_1_ntile_no_holdout'], "p": 0.05},
                    'out':{'success':False, 'true_value': "RANDOMIZED"}
                },
                {
                    'args': ['norm_1_1'],
                    'kwargs':{'partition_object': self.test_partitions['norm_0_1_kde_no_holdout'], "p": 0.05},
                    'out':{'success':False, 'true_value': "RANDOMIZED"}
                },
                {
                    'args': ['bimodal'],
                    'kwargs':{'partition_object': self.test_partitions['bimodal_auto_no_holdout'], "p": 0.05},
                    'out':{'success':True, 'true_value': "RANDOMIZED"}
                },
                {
                    'args': ['bimodal'],
                    'kwargs':{'partition_object': self.test_partitions['bimodal_kde_no_holdout'], "p": 0.05},
                    'out':{'success':True, 'true_value': "RANDOMIZED"}
                },
                {
                    'args': ['bimodal'],
                    'kwargs':{'partition_object': self.test_partitions['norm_0_1_auto_no_holdout'], "p": 0.05},
                    'out':{'success':False, 'true_value': "RANDOMIZED"}
                },
                {
                    'args': ['bimodal'],
                    'kwargs':{'partition_object': self.test_partitions['norm_0_1_uniform_no_holdout'], "p": 0.05},
                    'out':{'success':False, 'true_value': "RANDOMIZED"}
                }
            ]
        for t in T:
            out = self.D.expect_column_bootstrapped_ks_test_p_value_greater_than(*t['args'], **t['kwargs'])
            if out['success'] != t['out']['success']:
                print("Test case error:")
                print(t)
                print(out)
            self.assertEqual(out['success'], t['out']['success'])

    def test_expect_column_kl_divergence_less_than_continuous(self):
        T = [
                {
                    'args': ['norm_0_1'],
                    'kwargs':{"partition_object": self.test_partitions['norm_0_1_auto_both_holdout'], "threshold": 0.1},
                    'out':{'success':True, 'true_value': 0.0013326972943566281}
                },
                {
                    'args': ['norm_0_1'],
                    'kwargs':{"partition_object": self.test_partitions['norm_0_1_uniform_both_holdout'], "threshold": 0.1},
                    'out':{'success':True, 'true_value': 0.0013326972943566281}
                },
                {
                    'args': ['norm_0_1'],
                    'kwargs':{"partition_object": self.test_partitions['norm_0_1_ntile_no_holdout'], "threshold": 0.1},
                    'out':{'success':True, 'true_value': 0.00047210715733547086}
                },
                {
                    'args': ['norm_0_1'],
                    'kwargs':{"partition_object": self.test_partitions['norm_0_1_kde_no_holdout'], "threshold": 0.1},
                    'out':{'success':True, 'true_value': 0.039351496030977519}
                },
                {
                    'args': ['norm_1_1'],
                    'kwargs':{"partition_object": self.test_partitions['norm_0_1_auto_no_holdout'], "threshold": 0.1},
                    'out':{'success':False, 'true_value': 0.56801971244750626}
                },
                {
                    'args': ['norm_1_1'],
                    'kwargs':{"partition_object": self.test_partitions['norm_0_1_uniform_no_holdout'], "threshold": 0.1},
                    'out':{'success':False, 'true_value': 0.56801971244750626}
                },
                {
                    'args': ['norm_1_1'],
                    'kwargs':{"partition_object": self.test_partitions['norm_0_1_ntile_no_holdout'], "threshold": 0.1},
                    'out':{'success':False, 'true_value': 0.59398892510202805}
                },
                {
                    'args': ['norm_1_1'],
                    'kwargs':{"partition_object": self.test_partitions['norm_0_1_kde_no_holdout'], "threshold": 0.1},
                    'out':{'success':False, 'true_value': 0.52740442919069253}
                },
                {
                    'args': ['bimodal'],
                    'kwargs':{"partition_object": self.test_partitions['bimodal_auto_no_holdout'], "threshold": 0.1},
                    'out':{'success':True, 'true_value': 0.00023525468906568868}
                },
                # TODO: Consider changes that would allow us to detect this case
                # },
                # {
                #     'args': ['bimodal', self.kde_smooth_partition_bimodal],
                #     'kwargs':{"threshold": 0.1},
                #     'out':{'success':True, 'true_value': "NOTTESTED"}
                # }
                # {
                #     'args': ['bimodal', self.auto_partition_norm_0_1],
                #     'kwargs':{"threshold": 0.1},
                #     'out':{'success':False, 'true_value': "NOTTESTED"}
                # },
                # {
                #     'args': ['bimodal', self.uniform_partition_norm_0_1],
                #     'kwargs':{"threshold": 0.1},
                #     'out':{'success':False, 'true_value': "NOTTESTED"}
                # }
        ]
        for t in T:
            out = self.D.expect_column_kl_divergence_less_than(*t['args'], **t['kwargs'])
            #DEBUG
            if not np.allclose(out['success'],t['out']['success']):
                print(t)
                print(out)
            self.assertTrue(np.allclose(out['success'],t['out']['success']))

if __name__ == "__main__":
    unittest.main()


#
#
# D = ge.dataset.PandasDataSet(pd.DataFrame({
#         'norm_0_1': [1.86466312629, 0.598163544099, -0.686314834204, 0.309810770507, 0.535493433837, 0.163388690372,
#                      0.533467038791, -0.452458199477, 0.6128042924, -0.715518309472, 0.664479673133, -0.772286247287,
#                      0.511436045264, 0.34633454418, -0.0120123083816, 0.80128761377, -0.58090988221, 1.86359507146,
#                      0.27790976294, -0.598889926335, 0.970343670716, -0.935420463358, 0.587408513979, 2.44082854938,
#                      0.0640571988566, -0.270837720219, 2.21750329449, -2.1227831462, -0.0849270203954, 1.77350545171,
#                      0.0454059511527, 0.445408415592, -0.645070358507, -0.0466988453487, 0.17084825884, -0.863431330633,
#                      1.20048807471, 2.93308570168, -1.44365949831, 0.303071032935, -2.257130212, 0.961392676549,
#                      -1.05785027324, -0.0667377394975, 1.1756023638, -0.884354420199, -0.801116773263, -1.33853837383,
#                      -0.798936151909, -1.73055262605, -0.432665548456, -0.284001361107, 0.0316282125382, 0.652663447169,
#                      -1.31557048835, -1.67733409538, -0.61256916892, 0.493392559731, -0.802397345586, 0.327369399957,
#                      0.322043536354, -0.664144332536, -0.415760602992, 0.246630359218, -0.311962458627, -0.862671123374,
#                      -1.69452491381, -0.857326968618, 0.108412082394, -1.09221731045, -0.568866461897, 0.0781404238368,
#                      -1.23729933975, 0.206139556935, -0.785468954657, -1.32025788316, -0.499964240017, 0.973935377567,
#                      -0.134330562874, 2.54552750423, 2.03286081688, -0.111180501821, 1.30302344022, -0.693186665311,
#                      0.437228753768, 3.39283116651, 3.204047169, -0.197115713505, 0.0373215602847, -0.697221361091,
#                      0.11491291582, 1.43424066427, 1.67880904676, 0.485618802167, -1.05137961439, -1.05820018686,
#                      0.47852904757, -1.45340403056, 0.600652452415, -1.04030494199],
#         'norm_1_1': [1.44324420023, 0.318703328817, 0.326659323018, -0.00853083988706, -0.131200924224, -0.95815934865,
#                      0.8671359644, 0.678716257181, 1.73998241325, 0.922484823795, 2.46837082143, 1.87472003758,
#                      1.76922760927, 2.49011390834, -0.25067936756, 2.19127135191, 1.27798098691, 0.204024154836,
#                      0.746058633028, -0.0842233385703, 1.7805746489, 0.214567735825, 2.06080869581, 1.0058922947,
#                      0.923970034261, 0.249592329632, 2.31795445247, 2.2727513623, 1.45593832992, 0.792260141447,
#                      -0.325052005608, -1.11535526114, 1.09422143866, 1.19702987848, 1.45764688003, -0.168722282751,
#                      1.6023119983, 1.13957052946, 0.474104549922, -0.156615555875, 3.05623438296, 1.28393242696,
#                      1.90259696828, 2.50200777406, 1.52742799171, 1.16965302399, -1.25479093468, 1.00706349302,
#                      -0.250746586653, 2.62272778582, 1.21792956159, 1.59565143521, 0.619706299778, 1.10152082786,
#                      2.56010645341, 1.0752338423, 0.434729254654, 2.17822951433, 0.397421122663, 0.837208524156,
#                      1.37093347392, 1.40550424013, 1.80905560291, 0.145666312777, 0.581607338871, 0.455645589281,
#                      0.203186995041, 1.22214590342, 2.00306925364, 1.33978993846, 1.17329696762, 1.70627692637,
#                      0.521119268555, -0.20595865175, -0.229023040641, 1.7279013096, -0.276146064303, 0.216483189561,
#                      0.472667305604, 1.45401995249, 0.41045882653, 0.952315314732, 1.04162842785, 1.43471685118,
#                      1.98475438278, 0.085079569321, -0.404021557218, 1.20002328453, 1.26706265595, -0.29990472325,
#                      1.57787709361, 0.509401116218, 1.27848180567, -1.21622036979, 0.229887819635, 2.05072155713,
#                      0.238678031515, 1.5739707112, 2.45453289428, -0.306923527104],
#         'bimodal': [1.86466312629, 0.598163544099, -0.686314834204, 0.309810770507, 0.535493433837, 0.163388690372,
#                     0.533467038791, -0.452458199477, 0.6128042924, -0.715518309472, 0.664479673133, -0.772286247287,
#                     0.511436045264, 0.34633454418, -0.0120123083816, 0.80128761377, -0.58090988221, 1.86359507146,
#                     0.27790976294, -0.598889926335, 0.970343670716, -0.935420463358, 0.587408513979, 2.44082854938,
#                     0.0640571988566, -0.270837720219, 2.21750329449, -2.1227831462, -0.0849270203954, 1.77350545171,
#                     0.0454059511527, 0.445408415592, -0.645070358507, -0.0466988453487, 0.17084825884, -0.863431330633,
#                     1.20048807471, 2.93308570168, -1.44365949831, 0.303071032935, -2.257130212, 0.961392676549,
#                     -1.05785027324, -0.0667377394975, 1.1756023638, -0.884354420199, -0.801116773263, -1.33853837383,
#                     -0.798936151909, -1.73055262605, 9.5222778176, 11.146417443, 9.45136336205, 7.4307518559,
#                     8.75219811571, 10.3823936054, 9.29852974249, 9.15062691637, 10.218136547, 10.1593749219,
#                     10.9478414773, 10.8222547348, 10.9109809664, 9.80637003158, 10.3512708363, 9.7641908085,
#                     12.4560168885, 9.85159850706, 9.0510039736, 8.98913974317, 10.2162875826, 10.7151772009,
#                     10.6454321399, 9.30520007605, 11.2343561935, 9.69456264261, 10.1282854751, 11.072488419,
#                     10.5344858288, 8.71418259804, 10.7544541913, 8.00696908999, 12.0336736381, 10.3941841473,
#                     9.31572623073, 9.71341207949, 10.9358397156, 8.69433737287, 10.1700213822, 9.5851372466,
#                     10.382155099, 8.46211016028, 9.17304384738, 10.7920030101, 9.51702067279, 9.45003153565,
#                     9.97686637259, 10.2814734223, 10.3401131141, 10.2822279477],
#         'categorical_fixed': ['A', 'B', 'B', 'B', 'C', 'B', 'A', 'A', 'A', 'B', 'A', 'A', 'C', 'A', 'A', 'C', 'A', 'A',
#                               'A', 'A', 'A', 'C', 'B', 'A', 'A', 'A', 'A', 'B', 'A', 'B', 'A', 'B', 'B', 'B', 'A', 'A',
#                               'B', 'B', 'B', 'A', 'C', 'A', 'A', 'A', 'A', 'A', 'A', 'B', 'B', 'A', 'B', 'C', 'A', 'A',
#                               'C', 'A', 'A', 'A', 'A', 'A', 'B', 'B', 'B', 'A', 'C', 'A', 'B', 'A', 'C', 'A', 'A', 'B',
#                               'C', 'B', 'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A', 'A', 'B', 'A', 'A', 'A', 'C', 'B', 'B',
#                               'B', 'C', 'C', 'B', 'B', 'B', 'A', 'B', 'C', 'B']
#     }))
#
#     auto_partition_norm_0_1 = {'partition': [-np.inf,
#                                              -2.6332933169407764,
#                                              -2.0131947666448835,
#                                              -1.3930962163489908,
#                                              -0.7729976660530982,
#                                              -0.15289911575720527,
#                                              0.4671994345386876,
#                                              1.08729798483458,
#                                              1.707396535130473,
#                                              2.327495085426366,
#                                              2.9475936357222587,
#                                              np.inf],
#                                'weights': [5e-11,
#                                            0.039999999996,
#                                            0.059999999994,
#                                            0.19999999998,
#                                            0.19999999998,
#                                            0.20999999997899998,
#                                            0.129999999987,
#                                            0.129999999987,
#                                            0.019999999998,
#                                            0.009999999999,
#                                            5e-11]}
#
#     uniform_partition_norm_0_1 = {'partition': [-np.inf,
#                                                 -2.6332933169407764,
#                                                 -2.075204621674473,
#                                                 -1.5171159264081693,
#                                                 -0.9590272311418657,
#                                                 -0.40093853587556216,
#                                                 0.15715015939074162,
#                                                 0.715238854657045,
#                                                 1.2733275499233483,
#                                                 1.831416245189652,
#                                                 2.389504940455956,
#                                                 2.9475936357222587,
#                                                 np.inf],
#                                   'weights': [5e-11,
#                                               0.029999999997,
#                                               0.059999999994,
#                                               0.139999999986,
#                                               0.159999999984,
#                                               0.239999999976,
#                                               0.129999999987,
#                                               0.119999999988,
#                                               0.09999999999,
#                                               0.009999999999,
#                                               0.009999999999,
#                                               5e-11]}
#
#     ntile_partition_norm_0_1 = {'partition': [-np.inf,
#                                               -2.6332933169407764,
#                                               -1.3943762896604026,
#                                               -1.047440138882213,
#                                               -0.7517947976511563,
#                                               -0.3567665470936191,
#                                               -0.16002398579086452,
#                                               0.01982689385444154,
#                                               0.4048016653842277,
#                                               0.8914527488100399,
#                                               1.4054731749350613,
#                                               2.9475936357222587,
#                                               np.inf],
#                                 'weights': [5e-11,
#                                             0.09999999999,
#                                             0.09999999999,
#                                             0.09999999999,
#                                             0.09999999999,
#                                             0.09999999999,
#                                             0.09999999999,
#                                             0.09999999999,
#                                             0.09999999999,
#                                             0.09999999999,
#                                             0.09999999999,
#                                             5e-11]}
#
#     kde_smooth_partition_norm_0_1 = {'partition': [-np.inf,
#                                                    -2.9518131660681668,
#                                                    -2.5537059955146693,
#                                                    -2.1098951141462,
#                                                    -1.66608423277773,
#                                                    -1.2222733514092605,
#                                                    -0.7784624700407909,
#                                                    -0.33465158867232114,
#                                                    0.10915929269614821,
#                                                    0.5529701740646176,
#                                                    0.9967810554330874,
#                                                    1.4405919368015572,
#                                                    1.884402818170027,
#                                                    2.328213699538496,
#                                                    2.7720245809069657,
#                                                    3.2158354622754355,
#                                                    3.613942632828933,
#                                                    np.inf],
#                                      'weights': [5e-06,
#                                                  0.0060726918839516288,
#                                                  0.020159702471566624,
#                                                  0.044779595473493285,
#                                                  0.0697829560932192,
#                                                  0.10774307284494342,
#                                                  0.15748061329378657,
#                                                  0.1806295517178222,
#                                                  0.16000266569954627,
#                                                  0.11190716860971527,
#                                                  0.0696574752271613,
#                                                  0.0418971766847929,
#                                                  0.017086048802554706,
#                                                  0.005230547469766495,
#                                                  0.004509707252569568,
#                                                  0.0030412087531233034,
#                                                  5e-06]}
#
#     auto_partition_bimodal = {'partition': [-np.inf,
#                                             -1.9885596164412649,
#                                             -0.1393049254649641,
#                                             1.7099497655113367,
#                                             3.559204456487638,
#                                             5.4084591474639385,
#                                             7.257713838440239,
#                                             9.106968529416541,
#                                             10.956223220392841,
#                                             12.805477911369142,
#                                             np.inf],
#                               'weights': [5e-06,
#                                           0.25999740000000005,
#                                           0.20999790000000002,
#                                           0.029999700000000004,
#                                           0.0,
#                                           0.0,
#                                           0.10999890000000001,
#                                           0.3099969,
#                                           0.0799992,
#                                           5e-06]}
#
#     kde_smooth_partition_bimodal = {'partition': [-np.inf,
#                                                   -2.5857203722715107,
#                                                   -2.1876132017180137,
#                                                   -1.7770146963568272,
#                                                   -1.3664161909956405,
#                                                   -0.9558176856344538,
#                                                   -0.5452191802732673,
#                                                   -0.13462067491208085,
#                                                   0.27597783044910607,
#                                                   0.6865763358102925,
#                                                   1.097174841171479,
#                                                   1.5077733465326655,
#                                                   1.918371851893852,
#                                                   2.328970357255039,
#                                                   2.739568862616226,
#                                                   3.150167367977412,
#                                                   3.5607658733385987,
#                                                   3.9713643786997848,
#                                                   4.381962884060972,
#                                                   4.792561389422159,
#                                                   5.203159894783345,
#                                                   5.6137584001445315,
#                                                   6.024356905505718,
#                                                   6.434955410866905,
#                                                   6.845553916228091,
#                                                   7.256152421589277,
#                                                   7.666750926950465,
#                                                   8.077349432311651,
#                                                   8.487947937672837,
#                                                   8.898546443034023,
#                                                   9.309144948395211,
#                                                   9.719743453756397,
#                                                   10.130341959117583,
#                                                   10.540940464478771,
#                                                   10.951538969839957,
#                                                   11.362137475201143,
#                                                   11.77273598056233,
#                                                   12.183334485923517,
#                                                   12.593932991284703,
#                                                   13.00453149664589,
#                                                   13.402638667199387,
#                                                   np.inf],
#                                     'weights': [5e-06,
#                                                 0.085117371014848744,
#                                                 0.02506776527443055,
#                                                 0.02873235187904667,
#                                                 0.03185627365226694,
#                                                 0.03417853204333695,
#                                                 0.03549818100811645,
#                                                 0.03570295715903698,
#                                                 0.03478486399219424,
#                                                 0.03283995226766389,
#                                                 0.030052943420785797,
#                                                 0.02667046564267954,
#                                                 0.022968737201980848,
#                                                 0.019222104886145463,
#                                                 0.01567791453819715,
#                                                 0.012541110473316108,
#                                                 0.009969312126584185,
#                                                 0.008076541200709046,
#                                                 0.006941835338003493,
#                                                 0.00661808522604818,
#                                                 0.007136762912021137,
#                                                 0.008505729466014036,
#                                                 0.010699729920010798,
#                                                 0.013645964713579626,
#                                                 0.01720954099606588,
#                                                 0.02118488101776945,
#                                                 0.025298712461847596,
#                                                 0.029227938597402036,
#                                                 0.032631931567834366,
#                                                 0.03519461070045483,
#                                                 0.03666833905792489,
#                                                 0.03691032194272725,
#                                                 0.03590337068410482,
#                                                 0.03375634548727149,
#                                                 0.030684313954529998,
#                                                 0.026973065521541224,
#                                                 0.022935806317324395,
#                                                 0.01887084291587907,
#                                                 0.015027797804952423,
#                                                 0.038999177946332675,
#                                                 5e-06]}
#
#     categorical_partition = {
#         'partition': ['A', 'B', 'C'],
#         'weights': [0.54, 0.32, 0.14]
#     }