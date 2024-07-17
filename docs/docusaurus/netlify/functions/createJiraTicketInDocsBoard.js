const TITLE_MAX_CHARACTERS = 120;
const DOCUMENTATION_BOARD_ID = 10019;
const STORY_ISSUETYPE_ID = 10011;

const truncateDescription = (description) => {
    return description.length > TITLE_MAX_CHARACTERS ? description.substring(0, TITLE_MAX_CHARACTERS) + "..." : description;
}

const fullDescription = (description, name, email) => {
    return `Name: ${name || '-'}\nEmail: ${email || '-'}\n\n${description}`;
}

exports.handler = async (req, context) => {
    const { description, name, email } = JSON.parse(req.body);
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
                "description": fullDescription(description, name, email),
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
