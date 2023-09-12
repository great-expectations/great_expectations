# IDE Setup Tips

This document describes useful set-up tips for contributors to this repository. Feel free to suggest more useful changes to this.

## VS Code

Create a `.vscode` directory and add the following files to it:

_.vscode/extension.json_
```json
{
    // See https://go.microsoft.com/fwlink/?LinkId=827846 to learn about workspace recommendations.
	// Extension identifier format: ${publisher}.${name}. Example: vscode.csharp
	// List of extensions which should be recommended for users of this workspace.
	"recommendations": [
		"stateful.runme"
	],
	// List of extensions recommended by VS Code that should not be recommended for users of this workspace.
	"unwantedRecommendations": []
}
```

_.vscode/launch.json_
```json
{
    "version": "0.2.0",
    "configurations": [
        {
            "name": "GX Docusarus Docs",
            "type": "node-terminal",
            "request": "launch",
            "command": "invoke docs"
        },
        {
            "name": "GX Start MySQL Container",
            "type": "node-terminal",
            "request": "launch",
            "command": "docker-compose up -d",
            "cwd": "${workspaceFolder}/assets/docker/mysql"
        },
        {
            "name": "GX Start PostgreSQL Container",
            "type": "node-terminal",
            "request": "launch",
            "command": "docker-compose up -d",
            "cwd": "${workspaceFolder}/assets/docker/postgresql"
        }
    ]
}
```

_.vscode/settings.json_
```json
{
    "workbench.editorAssociations": {
        "CONTRIBUTING_WORKFLOWS.md": "runme",
        "CONTRIBUTING_CODE.md": "runme"
    }
}
```

## PyCharm

tbd.