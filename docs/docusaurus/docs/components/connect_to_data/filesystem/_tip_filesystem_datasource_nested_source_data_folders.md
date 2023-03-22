:::tip What if my source data files are split into different folders?

You can access files that are nested in folders under your Datasource's `base_path`!

If your source data files are split into multiple folders, you can use the folder that contains those folders as your `base_path`.  When you define a Data Asset for your Datasource, you can then include the folder path (relative to your `base_path`) in the regular expression that indicates which files to connect to. 

:::