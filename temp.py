from typing import List

test_suite_total_time: float = 2656.19
data_assistant_total_time: float = 0

with open("temp") as f:
    contents: List[str] = f.readlines()

for line in contents:
    if "data_assistant" in line.strip():
        performance = float(line.split("s", 1)[0])
        data_assistant_total_time += performance

data_assistant_proportion: float = (
    data_assistant_total_time / test_suite_total_time
) * 100

print(
    f"DataAssistant-related tests take {data_assistant_total_time:.2f}s, which makes up {data_assistant_proportion:.2f}% of the total time to run the whole suite."
)
