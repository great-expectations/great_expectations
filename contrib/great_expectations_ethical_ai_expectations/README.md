## Great Expectations Ethical AI Expectations
A collection of Expectations to validate for degradation, bias, and related Ethical Data concerns with Great Expectations.

### What Is Ethical Data / Ethical AI and Why Is It Important?

The buzz around ethical AI has been growing louder and louder. Companies are collecting and using [more data than ever before](https://www.statista.com/statistics/871513/worldwide-data-created/). And the influence of that data is growing just as fast—with
data powering decisions and actions across every industry.

It's critical that the algorithms fueling these decisions and actions are ethical ones. That's where ethical AI comes in. Ethical AI is artificial intelligence that adheres to strict guidelines (or codes of conduct) regarding fundamental values to ensure automated systems respond to situations in an ethical way. For example, an [algorithm that determines credit limits shouldn't discriminate against women](https://www.washingtonpost.com/business/2019/11/11/apple-card-algorithm-sparks-gender-bias-allegations-against-goldman-sachs/) by granting them lower limits.

### How Can Great Expectations Contribute to Ethical AI

Great Expectations facilitates ethical AI by making it faster and easier to turn codes of conduct into actual code that can be quickly applied to data. For example, one common source of bad model performance is degradation in accuracy over time. For example, an Expectation like `expect_column_label_and_predicted_pair_accuracy_to_be_between` could be used to guard against model degradation.

Author: [Great Expectations](https://github.com/great_expectations/great_expectations)

[PyPi Link](https://pypi/python.org/pypi/great_expectations_ethical_ai_expectations)
