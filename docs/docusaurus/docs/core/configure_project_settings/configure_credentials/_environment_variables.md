Environment variables provide the quickest way to securely set up your credentials. 

You can set environment variables by replacing the values in `<>` with your information and entering `export <VARIABLE_NAME>=<VALUE>` commands in the terminal or adding the commands to your `~/.bashrc` file.  If you use the `export` command from the terminal, the environment variables will not persist beyond the current session.  However, if you add them to your shell config file (`~/.bashrc` for Bash, `~./zshrc` for Z Shell), the variables will be exported each time you log in.

You can export credentials as individual components, or as an entire connection string (or token).  For example:

```bash title="Terminal, ~/.bashrc, or ~/.zshrc"
export MY_POSTGRES_USERNAME=<USERNAME>
export MY_POSTGRES_PASSWORD=<PASSWORD>
```

or:

```bash title="Terminal or ~/.bashrc"
export POSTGRES_CONNECTION_STRING=postgresql+psycopg2://<USERNAME>:<PASSWORD>@<HOST>:<PORT>/<DATABASE>
```

You can also reference your stored credentials within a stored connection string by wrapping their corresponding variable in `${` and `}`. For example:

```bash title="Terminal or ~/.bashrc"
export MY_POSTGRES_USERNAME=<USERNAME>
export MY_POSTGRES_PASSWORD=<PASSWORD>
export POSTGRES_CONNECTION_STRING=postgresql+psycopg2://${MY_POSTGRES_USERNAME}:${MY_POSTGRES_PASSWORD}@<HOST>:<PORT>/<DATABASE>
```

Because the dollar sign character `$` is used to indicate the start of a string substitution they should be escaped using a backslash `\` if they are part of your credentials. For example, if your password is `pa$$word` then in the previous examples you would use the command:

```bash title="Terminal or ~/.bashrc"
export MY_POSTGRES_PASSWORD=pa\$\$word
```
