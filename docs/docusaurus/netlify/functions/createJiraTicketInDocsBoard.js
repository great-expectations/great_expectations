const TITLE_MAX_CHARACTERS = 120;
const DOCUMENTATION_BOARD_ID = 1001988888;
const STORY_ISSUETYPE_ID = 10011;

function truncateDescription(description) {
    return description.length > TITLE_MAX_CHARACTERS ? description.substring(0, TITLE_MAX_CHARACTERS) + "..." : description;
}

exports.handler = async (req, context) => {
    const { description } = JSON.parse(req.body);
    const response = await fetch("https://greatexpectations.atlassian.net/rest/api/2/issue", {
        method: "POST",
        headers: {
            'Content-Type': 'application/json',
            'Authorization': 'Basic ' + Buffer.from(process.env.JIRA_API_USER + ":" + process.env.JIRA_API_TOKEN).toString('base64')
        },
        body: JSON.stringify({
            "fields": {
                "project": {
                    "id": DOCUMENTATION_BOARD_ID
                },
                "summary": truncateDescription(description),
                "description": description,
                "issuetype": {
                    "id": STORY_ISSUETYPE_ID
                }
            }
        })
    })

    const result = await response.json()

    return {
        statusCode: response.status,
        body: JSON.stringify(result)
    };
};
