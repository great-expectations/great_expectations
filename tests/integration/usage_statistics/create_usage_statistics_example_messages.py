from test_usage_statistics_messages import valid_usage_statistics_messages
import json

if __name__ == "__main__":

    msg = valid_usage_statistics_messages

    # remove top-level items from the messages
    keys = [k for k in msg.keys()]
    msg_list = []
    for key in keys:
        for item in msg[key]:
            msg_list.append(item)

    msg_json = json.dumps(msg_list)

    with open("usage_statistics_example_messages.json", "w") as outfile:
        json.dump(msg_list, outfile)
