import glob

json_files = glob.glob("tests/**/output/**/*.json", recursive=True)
html_files = glob.glob("tests/**/output/**/*.html", recursive=True)

html_list = ""
for f_ in html_files:
    html_list += '\t<li><a href="{}">{}</li>\n'.format(
        f_[6:],
        f_.split(".")[-2],
    )

json_list = ""
for f_ in json_files:
    json_list += '\t<li><a href="{}">{}</li>\n'.format(
        f_[6:],
        f_.split(".")[-2],
    )

html_file = f"""
<html>
<body>
  <h3>HTML</h3>
  <ul>
    {html_list}
  </ul>
  <br/><br/>
  <h3>JSON</h3>
  <ul>
    {json_list}
  </ul>
</body>
</html>
"""

print(html_file)
