const TITLE_MAX_CHARACTERS = 120;
const DOCUMENTATION_BOARD_ID = 10019;
const STORY_ISSUETYPE_ID = 10011;
const JIRA_ISSUE_ENDPOINT_URL = "https://greatexpectations.atlassian.net/rest/api/2/issue"
const TO_DO_STATE_ID = 291;

const truncateDescription = (description) => {
    return description.length > TITLE_MAX_CHARACTERS ? description.substring(0, TITLE_MAX_CHARACTERS) + "..." : description;
}

const formatOptionalValue = (value) => value || '-';

const fullDescription = (description, name, email, selectedValue) => {
    const formattedSelectedValue = selectedValue.replaceAll("-"," ");
    return `*Name:* ${formatOptionalValue(name)}\n*Email:* ${formatOptionalValue(email)}\n*Selected feedback type:* ${formattedSelectedValue}\n\n*Description:* ${description}`;
}

const httpPostRequest = async (url, body) => {
    return await fetch(url, {
        method: "POST",
        headers: {
            'Content-Type': 'application/json',
            'Authorization': 'Basic ' + Buffer.from(process.env.JIRA_API_USER + ":" + process.env.JIRA_API_TOKEN).toString('base64')
        },
        body: JSON.stringify(body)
    })
}

exports.handler = async (req, context) => {
    const { description, name, email, selectedValue } = JSON.parse(req.body);
    try {
        const response = await httpPostRequest(JIRA_ISSUE_ENDPOINT_URL, {
            "fields": {
                "project": {
                    "id": DOCUMENTATION_BOARD_ID
                },
                "summary": truncateDescription(description),
                "description": fullDescription(description, name, email, selectedValue),
                "issuetype": {
                    "id": STORY_ISSUETYPE_ID
                },
                "labels": [
                    "feedback_modal"
                ],
            }
        })

        const result = await response.json()

        await httpPostRequest(`${JIRA_ISSUE_ENDPOINT_URL}/${result.id}/transitions`, {
            "transition": {
                "id": TO_DO_STATE_ID
            }
        })

        return {
            statusCode: response.status,
            body: JSON.stringify(result)
        };

    } catch (error) {
        return Promise.reject(error);
    }
};
